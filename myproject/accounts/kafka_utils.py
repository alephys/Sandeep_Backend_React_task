
# accounts/kafka_utils.py
import os
import tempfile
import logging
from confluent_kafka.admin import AdminClient, AclBinding, AclBindingFilter, ResourceType, ResourcePatternType, AclOperation, AclPermissionType
from django.conf import settings
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

logger = logging.getLogger(__name__)

def get_admin_client_from_settings():
    """
    Returns a Confluent AdminClient using settings.KAFKA_CLIENT_CONFIG which must include
    bootstrap.servers, security.protocol, sasl.mechanism, sasl.username, sasl.password, ssl.* paths etc.
    """
    cfg = getattr(settings, 'KAFKA_CLIENT_CONFIG', None)
    if not cfg:
        raise RuntimeError("KAFKA_CLIENT_CONFIG missing in Django settings")
    return AdminClient(cfg)

def broadcast(group, event_type, data):
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        group,
        {
            "type": event_type,
            "data": data,
        }
    )

# Mapping convenience
OPERATION_MAP = {
    'READ': AclOperation.READ,
    'WRITE': AclOperation.WRITE,
    'DESCRIBE': AclOperation.DESCRIBE,
    'CREATE': AclOperation.CREATE,
    'DELETE': AclOperation.DELETE,
    'ALL': AclOperation.ALL,
    'READ_WRITE': AclOperation.READ  # treat READ_WRITE as creating two ACLs when needed
}

try:
    CLUSTER_TYPE = ResourceType.CLUSTER
except AttributeError:
    CLUSTER_TYPE = ResourceType.UNKNOWN  # fallback

RESOURCE_TYPE_MAP = {
    'TOPIC': ResourceType.TOPIC,
    'GROUP': ResourceType.GROUP,
    'CLUSTER': CLUSTER_TYPE,
}

def create_acl(principal: str, resource_type: str, resource_name: str, permission: str, host="*"):
    """
    Create ACL(s) on the cluster.
    - principal: e.g. "User:uid=kundana,cn=users,cn=accounts,dc=alephys,dc=com" or "User:kundana"
    - resource_type: 'TOPIC'|'GROUP'|'CLUSTER'
    - resource_name: name or '' for cluster
    - permission: one of PERMISSION_CHOICES ('READ','WRITE','ALL', ...)
    """
    client = get_admin_client_from_settings()
    rs_type = RESOURCE_TYPE_MAP[resource_type]
    op = permission.upper()

    acls_to_create = []
    if op == 'READ_WRITE' or op == 'READ/WRITE':
        ops = [AclOperation.READ, AclOperation.WRITE]
    elif op == 'ALL':
        ops = [AclOperation.ALL]
    else:
        ops = [OPERATION_MAP.get(op, AclOperation.READ)]

    for operation in ops:
        binding = AclBinding(
            resource_type=rs_type,
            resource_name=resource_name if resource_name else '',
            resource_pattern_type=ResourcePatternType.LITERAL,
            principal=principal,
            host=host,
            operation=operation,
            permission_type=AclPermissionType.ALLOW
        )
        acls_to_create.append(binding)

    futures = client.create_acls(acls_to_create)
    # Wait and check results
    errors = []
    for f in futures.values():
        try:
            f.result()
        except Exception as e:
            logger.exception("create_acl error")
            errors.append(str(e))
    return errors


def delete_acl(principal: str, resource_type: str, resource_name: str, operation=None, host="*"):
    """
    Delete ACL(s) matching the filter.
    """
    client = get_admin_client_from_settings()
    rs_type = RESOURCE_TYPE_MAP[resource_type]
    op_filter = None
    if operation:
        op_filter = OPERATION_MAP.get(operation.upper(), None)

    # AclBindingFilter requires placeholders for resource_type, resource_name, patternType, principal, host, operation, permissionType
    binding_filter = AclBindingFilter(
        resource_type=rs_type,
        resource_name=resource_name if resource_name else None,
        resource_pattern_type=ResourcePatternType.LITERAL,
        principal=principal,
        host=host,
        operation=op_filter,
        permission_type=AclPermissionType.ANY
    )
    futures = client.delete_acls([binding_filter])
    # returns dict mapping filter -> future(list of ACLBindings)
    errors = []
    for f in futures.values():
        try:
            res = f.result()
            # res is list of (AclBinding, error) tuples
        except Exception as e:
            errors.append(str(e))
    return errors


def list_acls(principal=None):
    """
    List ACLs. If principal given, filter by it.
    """
    client = get_admin_client_from_settings()
    # Wildcard filter to list everything
    binding_filter = AclBindingFilter(resource_type=ResourceType.ANY, resource_name=None, resource_pattern_type=ResourcePatternType.ANY,
                                     principal=principal, host=None, operation=None, permission_type=None)
    futures = client.describe_acls(binding_filter)
    try:
        res = futures.result() if hasattr(futures, 'result') else futures
        # Depending on confluent_kafka version describe_acls returns differently;
        # Typically it's a future -> call .result()
    except Exception as e:
        raise
    return res
