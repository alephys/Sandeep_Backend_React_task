import os
import re
import platform
import subprocess
from datetime import datetime
# from django.conf import settings
from myproject import settings
from accounts.models import AclEntry


# ------------------------------------------------------
# 1. Detect OS and select appropriate ACL CLI executable
# ------------------------------------------------------

def get_kafka_acl_cli_path():
    """
    Automatically finds the correct Kafka ACL script:
    - Windows → kafka-acls.bat
    - Linux / Mac → kafka-acls.sh
    """
    base_path = getattr(settings, "KAFKA_CLI_PATH", None)

    if not base_path:
        raise Exception("KAFKA_CLI_PATH is not configured in Django settings")

    if platform.system().lower().startswith("win"):
        return os.path.join(base_path, "bin", "windows", "kafka-acls.bat")
    else:
        return os.path.join(base_path, "bin", "kafka-acls.sh")


# ------------------------------------------------------
# 2. Run kafka-acls command and parse output (KRaft & ZK both work)
# ------------------------------------------------------
def fetch_kafka_acls():
    cli_path = get_kafka_acl_cli_path()

    bootstrap = settings.KAFKA_BOOTSTRAP_SERVER
    client_props = settings.KAFKA_CLIENT_PROPERTIES

    cmd = (
        f'"{cli_path}" '
        f'--bootstrap-server "{bootstrap}" '
        f'--list '
        f'--command-config "{client_props}"'
    )

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            shell=True,   # required for .bat
            check=True
        )

        print("RAW KAFKA ACL OUTPUT:\n", result.stdout)

        parsed = parse_kafka_acl_output(result.stdout)
        print("PARSED KAFKA ACLs:", parsed)

        return parsed

    except subprocess.CalledProcessError as e:
        print("Kafka ACL list FAILED:")
        print("STDOUT:", e.stdout)
        print("STDERR:", e.stderr)
        return []

    except Exception as e:
        print("Unexpected ACL error:", e)
        return []

# def fetch_kafka_acls():
#     cli_path = get_kafka_acl_cli_path()

#     bootstrap = settings.KAFKA_BOOTSTRAP_SERVER
#     client_props = settings.KAFKA_CLIENT_PROPERTIES

#     cmd = (
#         f'"{cli_path}" '
#         f'--bootstrap-server "{bootstrap}" '
#         f'--list '
#         f'--command-config "{client_props}"'
#     )

#     try:
#         result = subprocess.run(
#             cmd,
#             capture_output=True,
#             text=True,
#             shell=True,        # 🔥 REQUIRED for .bat files
#             check=True
#         )

#         print("RAW KAFKA ACL OUTPUT:\n", result.stdout)

#         return parse_kafka_acl_output(result.stdout)

#     except subprocess.CalledProcessError as e:
#         print("Kafka ACL list FAILED:")
#         print("STDOUT:", e.stdout)
#         print("STDERR:", e.stderr)
#         return []

#     except Exception as e:
#         print("Unexpected ACL error:", e)
#         return []
    


# def fetch_kafka_acls():
#     """
#     Executes kafka-acls tool to fetch ACLs directly from the Kafka cluster.
#     Works for ZooKeeper mode and KRaft mode.
#     """
#     cli_path = get_kafka_acl_cli_path()

#     bootstrap = settings.KAFKA_BOOTSTRAP_SERVER
#     client_props = settings.KAFKA_CLIENT_PROPERTIES  # path to client.properties

#     cmd = [
#         cli_path,
#         "--bootstrap-server", bootstrap,
#         "--list",
#         "--command-config", client_props
#     ]

#     try:
#         result = subprocess.run(
#             cmd, capture_output=True, text=True, check=True
#         )
#         return parse_kafka_acl_output(result.stdout)

#     except subprocess.CalledProcessError as e:
#         print("Kafka ACL list FAILED:", e.stderr)
#         return []

#     except Exception as e:
#         print("Unexpected ACL error:", e)
#         return []


# ------------------------------------------------------
# 3. Convert CLI output → structured Python dictionaries
# ------------------------------------------------------

def parse_kafka_acl_output(text):
    acls = []

    current_resource_type = None
    current_resource_name = None

    for raw_line in text.splitlines():
        line = raw_line.strip()

        # 1️⃣ Resource header line
        if "ResourcePattern(" in line:
            try:
                # Extract inside ResourcePattern(...)
                start = line.index("ResourcePattern(") + len("ResourcePattern(")
                end = line.index(")", start)
                inside = line[start:end]

                # inside example:
                # resourceType=TOPIC, name=NavyaReq091, patternType=LITERAL
                parts = dict(
                    kv.split("=", 1)
                    for kv in inside.split(", ")
                    if "=" in kv
                )

                current_resource_type = parts.get("resourceType", "").upper()
                current_resource_name = parts.get("name", "")

            except Exception:
                current_resource_type = None
                current_resource_name = None

            continue

        # 2️⃣ ACL entry line
        if line.startswith("(") and "principal=" in line:
            try:
                inside = line.strip("()")

                parts = dict(
                    kv.split("=", 1)
                    for kv in inside.split(", ")
                    if "=" in kv
                )

                acls.append({
                    "principal": parts.get("principal"),
                    "resource_type": current_resource_type or "UNKNOWN",
                    "resource_name": current_resource_name or "*",
                    "permission": parts.get("operation", "").upper(),
                    "host": parts.get("host", "*"),
                    "source": "kafka",
                })

            except Exception:
                continue

    return acls


# def parse_kafka_acl_output(text):
#     acls = []

#     current_resource_type = None
#     current_resource_name = None

#     for raw_line in text.splitlines():
#         line = raw_line.strip()

#         # 1️⃣ Resource header line
#         if "ResourcePattern(" in line:
#             match = re.search(
#                 r"resourceType=(?P<type>[^,]+),\s*name=(?P<name>[^,]+)",
#                 line
#             )
#             if match:
#                 current_resource_type = match.group("type").upper()
#                 current_resource_name = match.group("name")
#             continue

#         # 2️⃣ ACL entry line (starts with "(" after stripping)
#         if line.startswith("(") and "principal=" in line:
#             match = re.search(
#                 r"principal=(?P<principal>[^,]+),\s*"
#                 r"host=(?P<host>[^,]+),\s*"
#                 r"operation=(?P<operation>[^,]+),\s*"
#                 r"permissionType=(?P<permission>[^\)]+)",
#                 line
#             )

#             if not match:
#                 continue

#             acls.append({
#                 "principal": match.group("principal"),
#                 "resource_type": current_resource_type or "UNKNOWN",
#                 "resource_name": current_resource_name or "*",
#                 "permission": match.group("operation").upper(),
#                 "host": match.group("host"),
#                 "source": "kafka",
#             })

#     return acls


# def parse_kafka_acl_output(text):
#     acls = []

#     current_resource_type = None
#     current_resource_name = None

#     for raw_line in text.splitlines():
#         line = raw_line.strip()

#         # 1️⃣ Match resource header line EXACTLY as Kafka prints it
#         # Example:
#         # Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=NavyaReq091, patternType=LITERAL)`:
#         header_match = re.search(
#             r"ResourcePattern\(resourceType=(?P<type>[^,]+),\s*name=(?P<name>[^,]+)",
#             line
#         )

#         if header_match:
#             current_resource_type = header_match.group("type").upper()
#             current_resource_name = header_match.group("name")
#             continue

#         # 2️⃣ Match ACL entry line
#         # Example:
#         # (principal=User:uid=siva..., host=*, operation=ALL, permissionType=ALLOW)
#         acl_match = re.search(
#             r"principal=(?P<principal>[^,]+),\s*"
#             r"host=(?P<host>[^,]+),\s*"
#             r"operation=(?P<operation>[^,]+),\s*"
#             r"permissionType=(?P<permission>[^\)]+)",
#             line
#         )

#         if acl_match and current_resource_type:
#             acls.append({
#                 "principal": acl_match.group("principal"),
#                 "resource_type": current_resource_type,
#                 "resource_name": current_resource_name,
#                 "permission": acl_match.group("operation").upper(),
#                 "host": acl_match.group("host"),
#                 "source": "kafka",
#             })

#     return acls


# def parse_kafka_acl_output(text):
#     acls = []

#     current_resource_type = None
#     current_resource_name = None

#     for line in text.splitlines():
#         line = line.strip()

#         # 1️⃣ Detect resource header
#         # Example:
#         # Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=NavyaReq091, patternType=LITERAL)`:
#         if line.startswith("Current ACLs for resource"):
#             match = re.search(
#                 r"resourceType=(?P<type>[^,]+),\s*name=(?P<name>[^,]+)",
#                 line
#             )
#             if match:
#                 current_resource_type = match.group("type").upper()
#                 current_resource_name = match.group("name")
#             continue

#         # 2️⃣ Parse ACL entry line
#         if line.startswith("("):
#             match = re.search(
#                 r"principal=(?P<principal>[^,]+),\s*"
#                 r"host=(?P<host>[^,]+),\s*"
#                 r"operation=(?P<operation>[^,]+),\s*"
#                 r"permissionType=(?P<permission>[^\)]+)",
#                 line
#             )

#             if not match:
#                 continue

#             acls.append({
#                 "principal": match.group("principal"),
#                 "resource_type": current_resource_type or "UNKNOWN",
#                 "resource_name": current_resource_name or "*",
#                 "permission": match.group("operation").upper(),
#                 "host": match.group("host"),
#                 "source": "kafka",
#             })

#     return acls


# def parse_kafka_acl_output(text):
#     acls = []

#     for line in text.splitlines():
#         line = line.strip()

#         if not line.startswith("("):
#             continue

#         # Example line:
#         # (principal=User:CN=..., host=*, operation=ALL, permissionType=ALLOW)

#         match = re.search(
#             r"principal=(?P<principal>[^,]+),\s*"
#             r"host=(?P<host>[^,]+),\s*"
#             r"operation=(?P<operation>[^,]+),\s*"
#             r"permissionType=(?P<permission>[^\)]+)",
#             line
#         )

#         if not match:
#             continue

#         acls.append({
#             "principal": match.group("principal"),
#             "resource_type": "CLUSTER",   # Kafka does NOT repeat resource here
#             "resource_name": "*",
#             "permission": match.group("operation"),
#             "host": match.group("host"),
#             "source": "kafka",
#         })

#     return acls

# def parse_kafka_acl_output(text):
#     acls = []

#     for line in text.splitlines():
#         parts = line.split()

#         # Expected format:
#         # User:alice has Allow permission on Topic:orders from host *
#         if len(parts) < 8:
#             continue

#         try:
#             principal = parts[1]        # User:alice
#             operation = parts[3]        # READ / WRITE
#             permission = parts[4]       # Allow / Deny
#             resource_full = parts[6]    # Topic:orders
#             host = parts[-1]            # *

#             if ":" in resource_full:
#                 resource_type, resource_name = resource_full.split(":")
#             else:
#                 resource_type, resource_name = resource_full, ""

#             acls.append({
#                 "principal": principal,
#                 "resource_type": resource_type.upper(),
#                 "resource_name": resource_name,
#                 "permission": operation.upper(),
#                 "host": host,
#                 "source": "kafka"
#             })

#         except Exception:
#             continue

#     return acls


# ------------------------------------------------------
# 4. Merge DB ACLs + Kafka ACLs
# ------------------------------------------------------

def merge_acl_sources():
    """
    Returns merged ACLs:
    - DB-only ACLs (source = db)
    - Kafka-only ACLs (source = kafka)
    - Synced ACLs that exist in both (source = db+kafka)
    """
    db_acls = AclEntry.objects.select_related("granted_by").all()
    kafka_acls = fetch_kafka_acls()

    merged = []
    seen = set()

    # Process DB ACLs first
    for acl in db_acls:
        key = f"{acl.principal}|{acl.resource_type}|{acl.resource_name}|{acl.permission}"

        merged_entry = {
            "id": acl.id,
            "principal": acl.principal,
            "resource_type": acl.resource_type,
            "resource_name": acl.resource_name,
            "permission": acl.permission,
            "host": "*",
            "granted_by": acl.granted_by.username if acl.granted_by else "-",
            "created_at": acl.created_at.isoformat(),
            "source": "db"
        }

        # Check if this DB ACL exists in Kafka
        for kac in kafka_acls:
            if (
                kac["principal"] == acl.principal and
                kac["resource_type"] == acl.resource_type and
                kac["resource_name"] == acl.resource_name and
                kac["permission"] in (acl.permission, "ALL")
            ):
                merged_entry["source"] = "db+kafka"

        merged.append(merged_entry)
        seen.add(key)

    # Add Kafka-only ACLs
    for kac in kafka_acls:
        key = f"{kac['principal']}|{kac['resource_type']}|{kac['resource_name']}|{kac['permission']}"

        if key not in seen:
            merged.append({
                "id": None,
                "principal": kac["principal"],
                "resource_type": kac["resource_type"],
                "resource_name": kac["resource_name"],
                "permission": kac["permission"],
                "host": kac["host"],
                "granted_by": "-",
                "created_at": "-",
                "source": "kafka"
            })

    return merged
