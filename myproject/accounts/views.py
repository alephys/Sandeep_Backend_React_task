#accounts/views.py
from .utils import broadcast_to_admin, broadcast_to_users

from django.utils.timezone import make_aware
from datetime import datetime

from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render, redirect
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.contrib import messages
from django.utils import timezone
from django.contrib.auth.models import User
from myproject import settings
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.contrib.auth.decorators import login_required, user_passes_test

from accounts.acls import merge_acl_sources

import re

import subprocess

import ldap

import json

import logging

from .models import LogEntry, LoginEntry, Topic, TopicRequest, AclEntry
# from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, AclBinding, ResourcePattern, ResourceType, AclOperation, AclPermissionType

# from confluent_kafka.admin import (
#     AdminClient,
#     NewTopic,
#     NewPartitions,
#     AclBinding,
#     ResourceType,
#     AclOperation,
#     AclPermissionType,
#     AclBindingFilter, 
# )

from confluent_kafka.admin import (
    AdminClient,
    NewTopic,
    NewPartitions,
    AclBinding,
    AclBindingFilter,
    ResourceType,
    ResourcePatternType,
    AclOperation,
    AclPermissionType
)

from ldap3 import Server, Connection, ALL, SUBTREE

from .kafka_utils import create_acl, delete_acl

# KAFKA_BOOTSTRAP = [
#     'sandeep.infra.alephys.com:12091',
#     'sandeep.infra.alephys.com:12092',
# ]

# conf = getattr(settings, 'KAFKA_CLIENT_CONFIG', None)

# getattr(settings, 'AUTH_LDAP_SERVER_URI', None)

# navyanode3 Broker

print("DEBUG PATH:", settings.KAFKA_CLI_PATH)

conf = {
    'bootstrap.servers': 'navyanode3.infra.alephys.com:9094',
    'security.protocol': 'SSL',
    'ssl.ca.location': r'C:\Users\91913\Desktop\AlephysReact\backend\navyaNode3\UI-broker-ca.pem',
    'ssl.certificate.location': r'C:\Users\91913\Desktop\AlephysReact\backend\navyaNode3\client-cert.pem',
    'ssl.key.location': r'C:\Users\91913\Desktop\AlephysReact\backend\navyaNode3\client-key.pem',
    'ssl.endpoint.identification.algorithm': 'none',
}

#sansnode1 Broker, if, ssl.clent.auth=requested
# conf = {
#     'bootstrap.server': 'sansnode1.infra.alephys.com:9091',
#     'security.protocol': 'SSL',
#     'ssl.ca.location': r'C:\Users\91913\Desktop\AlephysReact\backend\sansnode1_kundana\trustCa-cert.pem',
#     'ssl.endpoint.identification.algorithm': 'none',
# }

#sansnode1 Broker, if, ssl.clent.auth=required
# conf = {
#     'bootstrap.servers': 'sansnode1.infra.alephys.com:9091',
#     'security.protocol': 'SSL',
#     'ssl.ca.location': r'C:\Users\91913\Desktop\AlephysReact\backend\sansnode1_kundana\trustCa-cert.pem',
#     'ssl.certificate.location': r'C:\Users\91913\Desktop\AlephysReact\backend\sansnode1_kundana\client-cert.pem',
#     'ssl.key.location': r'C:\Users\91913\Desktop\AlephysReact\backend\sansnode1_kundana\client-key.pem',
#     'ssl.key.password': '4lph@123',
#     'ssl.endpoint.identification.algorithm': 'none',
# }

logger = logging.getLogger(__name__)

# One-time cleanup of existing topics (run once, then comment out)
# if Topic.objects.exists():
#     admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
#     for topic in Topic.objects.all():
#         try:
#             admin_client.delete_topics([topic.name])
#             logger.info(f"Cleaned up existing Kafka topic '{topic.name}'")
#         except Exception as e:
#             logger.error(f"Failed to clean up Kafka topic '{topic.name}': {e}")
#         topic.delete()
#     logger.info("All existing topics cleaned up from database")

# @csrf_exempt
# def login_view_api(request):
#     if request.method == "POST":
#         username = request.POST.get("username")
#         password = request.POST.get("password")

#         user = authenticate(request, username=username, password=password)

#         if user is not None:
#             login(request, user)
#             logger.info(f"User {username} logged in successfully")

#             LoginEntry.objects.create(
#                 username=username,
#                 login_time=timezone.now(),
#                 success=True
#             )

#             # Determine role
#             role = "admin" if user.is_superuser else "user"

#             return JsonResponse({
#                 "success": True,
#                 "message": "Login successful",
#                 "role": role
#             })

#         else:
#             logger.warning(f"Failed login attempt for {username}")
#             LoginEntry.objects.create(
#                 username=username,
#                 login_time=timezone.now(),
#                 success=False
#             )
#             return JsonResponse({
#                 "success": False,
#                 "message": "Invalid credentials"
#             })

#     return JsonResponse({"error": "Invalid request"}, status=400)

@csrf_exempt
def login_view_api(request):
    if request.method == "POST":

        # Accept JSON & form-data
        try:
            body = json.loads(request.body.decode("utf-8"))
        except:
            body = {}

        username = body.get("username") or request.POST.get("username")
        password = body.get("password") or request.POST.get("password")

        user = authenticate(request, username=username, password=password)

        if user is not None:
            login(request, user)
            logger.info(f"User {username} logged in successfully")

            LoginEntry.objects.create(
                username=username,
                login_time=timezone.now(),
                success=True
            )

            role = "admin" if user.is_superuser else "user"

            return JsonResponse({
                "success": True,
                "message": "Login successful",
                "role": role
            })

        else:
            logger.warning(f"Failed login attempt for {username}")
            LoginEntry.objects.create(
                username=username,
                login_time=timezone.now(),
                success=False
            )
            return JsonResponse({
                "success": False,
                "message": "Invalid credentials"
            })

    return JsonResponse({"error": "Invalid request"}, status=400)


def login_view(request):
    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")
        user = authenticate(request, username=username, password=password)
        if user is not None:
            login(request, user)
            logger.info(f"User {username} logged in successfully")
            LoginEntry.objects.create(
                username=username,
                login_time=timezone.now(),
                success=True
            )
            if user.is_superuser:
                return JsonResponse({"success": True, "message": "Login successful", "redirect": "/admin_dashboard/"})
            return JsonResponse({"success": True, "message": "Login successful", "redirect": "/home/"})
        else:
            logger.warning(f"Failed login attempt for {username}")
            LoginEntry.objects.create(
                username=username,
                login_time=timezone.now(),
                success=False
            )
            return JsonResponse({"success": False, "message": "Invalid credentials"})
    return render(request, "login.html")

@csrf_exempt
def logout_view_api(request):
    if request.user.is_authenticated:
        logger.info(f"User {request.user.username} logged out")
        logout(request)
        request.session.flush()
    return JsonResponse({"success": True, "message": "Logged out successfully"})

@login_required
def logout_view(request):
    if request.user.is_authenticated:
        logger.info(f"User {request.user.username} logged out")
        logout(request)
        request.session.flush()
    return redirect("login")

# @csrf_exempt
# def home_api(request):
#     user = request.user

#     if not user.is_authenticated:
#         return JsonResponse({"success": False, "message": "User not authenticated."}, status=403)

#     # Handle GET requests
#     if request.method == "GET":
#         print(" home_api called by:", user.username)

#         topics = Topic.objects.filter(is_active=True, created_by=user)
#         approved_requests = TopicRequest.objects.filter(requested_by=user, status="APPROVED")

#         # Identify approved but uncreated topics
#         uncreated_requests = [
#             {
#                 "id": req.id,
#                 "topic_name": req.topic_name,
#                 "partitions": req.partitions,
#                 "status": req.status,
#             }
#             for req in approved_requests
#             if not Topic.objects.filter(name=req.topic_name, is_active=True, created_by=user).exists()
#         ]

#         created_topics = [
#             {
#                 "id": topic.id,
#                 "name": topic.name,
#                 "partitions": topic.partitions,
#             }
#             for topic in topics
#         ]

#         # user role
#         role = "admin" if user.is_superuser else "user"

#         data = {
#             "success": True,
#             "username": user.username,
#             "role": role,  # Include role in response
#             "uncreated_requests": uncreated_requests,
#             "created_topics": created_topics,
#         }
#         return JsonResponse(data, safe=False)

#     elif request.method == "POST":
#         try:
#             data = json.loads(request.body.decode("utf-8"))
#         except json.JSONDecodeError:
#             return JsonResponse({"success": False, "message": "Invalid JSON payload."}, status=400)

#         topic_name = data.get("topic_name", "").strip()
#         partitions = data.get("partitions")

#         if not topic_name or not partitions:
#             return JsonResponse({"success": False, "message": "Please fill all fields."}, status=400)

#         try:
#             partitions = int(partitions)
#             if partitions < 1:
#                 return JsonResponse({"success": False, "message": "Partitions must be at least 1."}, status=400)

#             # Check duplicate pending request
#             if TopicRequest.objects.filter(
#                 topic_name=topic_name,
#                 requested_by=user,
#                 status="PENDING"
#             ).exists():
#                 return JsonResponse({"success": False, "message": "You already have a pending request for this topic."}, status=400)

#             # Create new request
#             TopicRequest.objects.create(
#                 topic_name=topic_name,
#                 partitions=partitions,
#                 requested_by=user,
#                 status="PENDING",
#             )
#             # --- NEW ---
#             # broadcast_to_admin({"type": "pending_request_added"})
#             broadcast_to_admin({
#                 "event": "new_request",
#                 "topic_name": topic_name,
#                 "partitions": partitions,
#                 "requested_by": request.user.username,
#             })

#             broadcast_to_users({
#                 "event": "user_refresh"
#             })

#             logger.info(f"Topic request for '{topic_name}' submitted by {user.username}")
#             return JsonResponse({"success": True, "message": "Topic creation request submitted. Waiting for admin approval."})

#         except ValueError:
#             return JsonResponse({"success": False, "message": "Invalid number of partitions."}, status=400)

#     else:
#         return JsonResponse({"success": False, "message": "Unsupported request method."}, status=400)   

@csrf_exempt
def home_api(request):
    user = request.user

    if not user.is_authenticated:
        return JsonResponse({"success": False, "message": "User not authenticated."}, status=403)

    # Handle GET requests
    if request.method == "GET":
        print(" home_api called by:", user.username)

        topics = Topic.objects.filter(is_active=True, created_by=user)
        # approved_requests = TopicRequest.objects.filter(requested_by=user, status="APPROVED")
        approved_requests = TopicRequest.objects.filter(requested_by=user).exclude(status="COMPLETED")

        # Identify approved but uncreated topics
        uncreated_requests = [
            {
                "id": req.id,
                "topic_name": req.topic_name,
                "partitions": req.partitions,
                "status": req.status,
            }
            for req in approved_requests
            if not Topic.objects.filter(name=req.topic_name, is_active=True, created_by=user).exists()
        ]

        created_topics = [
            {
                "id": topic.id,
                "name": topic.name,
                "partitions": topic.partitions,
            }
            for topic in topics
        ]

        # user role
        role = "admin" if user.is_superuser else "user"

        data = {
            "success": True,
            "username": user.username,
            "role": role,  # Include role in response
            "uncreated_requests": uncreated_requests,
            "created_topics": created_topics,
        }
        return JsonResponse(data, safe=False)

    elif request.method == "POST":
        try:
            data = json.loads(request.body.decode("utf-8"))
        except json.JSONDecodeError:
            return JsonResponse({"success": False, "message": "Invalid JSON payload."}, status=400)

        topic_name = data.get("topic_name", "").strip()
        partitions = data.get("partitions")


        request_type = data.get("request_type", "CREATE")

        if not topic_name or not partitions:
            return JsonResponse({"success": False, "message": "Please fill all fields."}, status=400)

        try:
            partitions = int(partitions)
            if partitions < 1:
                return JsonResponse({"success": False, "message": "Partitions must be at least 1."}, status=400)

            # Check duplicate pending request
            if TopicRequest.objects.filter(
                topic_name=topic_name,
                requested_by=user,
                status="PENDING"
            ).exists():
                return JsonResponse({"success": False, "message": "You already have a pending request for this topic."}, status=400)

            # Create new request
            # TopicRequest.objects.create(
            #     topic_name=topic_name,
            #     partitions=partitions,
            #     requested_by=user,
            #     status="PENDING",
            # )

            TopicRequest.objects.create(
                topic_name=topic_name,
                partitions=partitions,
                requested_by=user,
                status="PENDING",
                request_type=request_type
            )

            # --- NEW ---
            # broadcast_to_admin({"type": "pending_request_added"})
            # broadcast_to_admin({
            #     "event": "new_request",
            #     "topic_name": topic_name,
            #     "partitions": partitions,
            #     "requested_by": request.user.username,
            # })
            broadcast_to_admin({
                "event": "new_request",
                "request_type": request_type,
                "topic_name": topic_name,
                "requested_by": request.user.username,
            })


            broadcast_to_users({
                "event": "user_refresh"
            })

            logger.info(f"Topic request for '{topic_name}' submitted by {user.username}")
            return JsonResponse({"success": True, "message": "Topic creation request submitted. Waiting for admin approval."})

        except ValueError:
            return JsonResponse({"success": False, "message": "Invalid number of partitions."}, status=400)

    else:
        return JsonResponse({"success": False, "message": "Unsupported request method."}, status=400)   
        
@login_required
def home(request):
    if request.user.is_superuser:
        return redirect("admin_dashboard")
    context = {
        "topics": Topic.objects.filter(is_active=True, created_by=request.user),
        "username": request.user.username,
    }
    if request.method == "POST":
        topic_name = request.POST.get("topic_name").strip()
        partitions = request.POST.get("partitions")
        if topic_name and partitions:
            try:
                partitions = int(partitions)
                if partitions < 1:
                    context["error"] = "Partitions must be at least 1."
                elif TopicRequest.objects.filter(topic_name=topic_name, requested_by=request.user, status='PENDING').exists():
                    context["error"] = "You already have a pending request for this topic."
                else:
                    TopicRequest.objects.create(
                        topic_name=topic_name,
                        partitions=partitions,
                        requested_by=request.user
                    )
                    context["success"] = "Topic creation request submitted. Waiting for admin approval."
                    logger.info(f"Topic request for {topic_name} submitted by {request.user.username}")
            except ValueError:
                context["error"] = "Invalid number of partitions."
        else:
            context["error"] = "Please fill all fields."
    
    # Reset approved requests to ensure no stale data
    approved_requests = TopicRequest.objects.filter(requested_by=request.user, status='APPROVED')
    uncreated_requests = []
    for req in approved_requests:
        if not Topic.objects.filter(name=req.topic_name, is_active=True, created_by=request.user).exists():
            uncreated_requests.append(req)
    context["uncreated_requests"] = uncreated_requests

    # Add created topics to context
    context["created_topics"] = Topic.objects.filter(is_active=True, created_by=request.user)

    return render(request, "home.html", context)


@csrf_exempt
def admin_dashboard_api(request):
    if request.method == "GET":
        print("admin_dashboard_api called by:", request.user)

        # ----- 1) Fetch topics from Kafka -----
        kafka_topics = get_kafka_topics(include_internal=True)

        # ----- 2) Fetch topics from DB for enrichment -----
        db_topics_qs = Topic.objects.filter(is_active=True).values(
            "id", "name", "partitions", "created_by__username", "created_at",
        )
        db_topics_by_name = {t["name"]: t for t in db_topics_qs}

        # ----- 3) Merge Kafka topics + DB topics -----
        created_topics = []
        for kt in kafka_topics:
            db_topic = db_topics_by_name.get(kt["name"])

            created_topics.append(
                {
                    # UI-created topics will have a DB id; CLI/internal won’t
                    "id": db_topic["id"] if db_topic else None,
                    "name": kt["name"],
                    "partitions": kt["partitions"],
                    # If we know who created it (UI), show that;
                    # otherwise mark as CLI / System / Unknown
                    "created_by__username": (
                        db_topic["created_by__username"]
                        if db_topic and db_topic["created_by__username"]
                        else "CLI / System"
                    ),
                    "is_internal": kt["is_internal"],

                    "created_at": db_topic["created_at"] if db_topic else None,
                }
            )

        created_topics.sort(
            key=lambda t: t["created_at"] or make_aware(datetime.min),
            reverse=True,   # 👈 latest first
        )
        data = {
            "pending_requests": list(
                TopicRequest.objects.filter(status="PENDING")
                .order_by("-requested_at")
                .values("id", "topic_name", "partitions", "requested_by__username", "request_type")
            ),
            "created_topics": created_topics,
            "username": getattr(request.user, "username", "admin"),
        }

        return JsonResponse(data, safe=False)
    # if request.method == "GET":
    #     print("admin_dashboard_api called by:", request.user)
    #     data = {
    #         "pending_requests": list(
    #             TopicRequest.objects.filter(status="PENDING")
    #             .order_by("-requested_at")
    #             .values("id", "topic_name", "partitions", "requested_by__username")
    #         ),
    #         "created_topics": list(
    #             Topic.objects.filter(is_active=True)
    #             .values("id", "name", "partitions", "created_by__username")
    #         ),
    #         "username": getattr(request.user, "username", "admin"),
    #     }
    #     return JsonResponse(data)

    elif request.method == "POST":
        try:
            data = json.loads(request.body.decode("utf-8"))
            topic_name = data.get("topic_name", "").strip()
            partitions = data.get("partitions")

            if not topic_name or not partitions:
                return JsonResponse({"success": False, "message": "Missing topic name or partitions."}, status=400)

            partitions = int(partitions)
            if partitions < 1:
                return JsonResponse({"success": False, "message": "Partitions must be at least 1."}, status=400)

            if Topic.objects.filter(name=topic_name, is_active=True).exists():
                return JsonResponse({"success": False, "message": f"Topic '{topic_name}' already exists."}, status=400)

            # Kafka topic creation
            # admin_client = AdminClient({'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)})
            admin_client = AdminClient(conf)
            new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
            fs = admin_client.create_topics([new_topic])
            for _, f in fs.items():
                f.result()

            # Save to DB
            Topic.objects.create(
                name=topic_name,
                partitions=partitions,
                created_by=request.user,
                production="Active",
                consumption="Active",
                followers=1,
                observers=0,
                last_produced=timezone.now(),
            )
            # --- NEW ---
            # broadcast_to_users({"type": "topic_created_by_admin"})
            # broadcast_to_admin({"type": "admin_refresh"})
            # Notify all users
            broadcast_to_users({
                "event": "topic_created",
                "topic_name": topic_name,
                "partitions": partitions,
                "created_by": request.user.username,
            })

            # Notify all admins
            broadcast_to_admin({
                "event": "admin_refresh"
            })


            # Return updated dashboard data
            updated_data = {
                "success": True,
                "message": f"Topic '{topic_name}' created successfully!",
                "pending_requests": list(
                    TopicRequest.objects.filter(status="PENDING")
                    .order_by("-requested_at")
                    .values("id", "topic_name", "partitions", "requested_by__username")
                ),
                "created_topics": list(
                    Topic.objects.filter(is_active=True)
                    .values("id", "name", "partitions", "created_by__username")
                ),
            }

            return JsonResponse(updated_data)

        except Exception as e:
            logger.error(f"Error creating topic: {e}")
            return JsonResponse({"success": False, "message": str(e)}, status=500)
        

def get_kafka_topics(include_internal=True):
    """
    Returns a list of topics from Kafka:
    [{ "name": str, "partitions": int, "is_internal": bool }, ...]
    """
    admin_client = AdminClient(conf)
    md = admin_client.list_topics(timeout=10)

    topics = []
    for name, t in md.topics.items():
        is_internal = name.startswith("_") or name.startswith("__")

        if not include_internal and is_internal:
            continue

        topics.append(
            {
                "name": name,
                "partitions": len(t.partitions),
                "is_internal": is_internal,
            }
        )

    return topics


@login_required
def admin_dashboard(request):
    if not request.user.is_superuser:
        return redirect("home")
    context = {
        "topics": Topic.objects.filter(is_active=True, created_by=request.user),
        "username": request.user.username,
        "pending_requests": TopicRequest.objects.filter(status='PENDING').order_by('-requested_at'),
        "created_topics": Topic.objects.filter(is_active=True),
    }
    if request.method == "POST":
        topic_name = request.POST.get("topic_name").strip()
        partitions = request.POST.get("partitions")
        if topic_name and partitions:
            try:
                partitions = int(partitions)
                if partitions < 1:
                    context["error"] = "Partitions must be at least 1."
                elif Topic.objects.filter(name=topic_name, is_active=True).exists():
                    context["error"] = f"Topic '{topic_name}' already exists."
                else:
                    try:
                        # admin_client = AdminClient({
                        #     'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)
                        # })
                        admin_client = AdminClient(conf);
                        new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
                        admin_client.create_topics([new_topic])
                        logger.info(f"Admin created Kafka topic '{topic_name}' with {partitions} partitions")
                    except Exception as e:
                        logger.error(f"Kafka topic creation failed: {e}")
                        context["error"] = f"Failed to create topic in Kafka: {str(e)}"
                        return render(request, "admin.html", context)
                    Topic.objects.create(
                        name=topic_name,
                        partitions=partitions,
                        created_by=request.user,
                        production="Active",
                        consumption="Active",
                        followers=1,
                        observers=0,
                        last_produced=timezone.now()
                    )
                    logger.info(f"Admin created topic '{topic_name}' in Django")
                    messages.success(request, f"Topic '{topic_name}' created successfully!")
                    return redirect("admin_dashboard")
            except ValueError:
                context["error"] = "Invalid number of partitions."
        else:
            context["error"] = "Please fill all fields."
    return render(request, "admin.html", context)

def is_admin(user):
    # Consider Django superuser or your own role check
    return user.is_superuser

@csrf_exempt
@login_required
@user_passes_test(is_admin)
def admin_stats_api(request):
    """Return all dashboard stats for admin"""
    stats = {
        "topics_admin": Topic.objects.filter(created_by__is_superuser=True).count(),
        "topics_users": Topic.objects.filter(created_by__is_superuser=False).count(),
        "requests_total": TopicRequest.objects.count(),
        "requests_approved": TopicRequest.objects.filter(status="APPROVED").count(),
        "requests_declined": TopicRequest.objects.filter(status="DECLINED").count(),
        "acls_total": AclEntry.objects.count(),
    }
    return JsonResponse({"success": True, "stats": stats})


# @csrf_exempt
# def list_acls_api(request):
#     try:
#         admin_client = AdminClient(conf)

#         # Correct enum usage
#         acl_filter = AclBindingFilter(
#             resource_type=ResourceType.ANY,
#             name=None,
#             pattern_type=ResourcePatternType.ANY,
#             principal=None,
#             host=None,
#             operation=AclOperation.ANY,
#             permission_type=AclPermissionType.ANY
#         )

#         acls = admin_client.describe_acls(acl_filter)
#         acl_list = []

#         for acl in acls.result():
#             # Access attributes that actually exist
#             acl_list.append({
#                 "resource_type": str(acl.resource_pattern.resource_type),
#                 "resource_name": acl.resource_pattern.name,
#                 "pattern_type": str(acl.resource_pattern.pattern_type),
#                 "principal": acl.entry.principal,
#                 "permission": str(acl.entry.permission_type),
#                 "operation": str(acl.entry.operation),
#                 "host": acl.entry.host,
#             })

#         return JsonResponse({"acls": acl_list})

#     except Exception as e:
#         print("list_acls_api error:", e)
#         return JsonResponse({"error": str(e)}, status=500)

# conf = {
#     'bootstrap.servers': 'navyanode3.infra.alephys.com:9094',
#     'security.protocol': 'SSL',
#     'ssl.ca.location': r'C:\Users\91913\Desktop\AlephysReact\backend\navya\ca-cert.pem',
#     'ssl.endpoint.identification.algorithm': 'none',
# }

# @csrf_exempt
# def list_acls_api(request):
#     try:
#         acls = merge_acl_sources()
#         return JsonResponse({"acls": acls})
#     except Exception as e:
#         return JsonResponse({"error": str(e)}, status=500)

def list_acls_api(request):
    acls = merge_acl_sources()
    print("FINAL ACLs SENT TO UI:", acls)
    return JsonResponse({"acls": acls})

    
# @csrf_exempt
# def list_acls_api(request):
#     try:
#         # Path to your kafka-acls.sh script
#         kafka_acls_path = "/path/to/kafka/bin/kafka-acls.sh"
#         bootstrap_server = "navyanode3.infra.alephys.com:9094"  # replace with your broker

#         # Run the CLI command to list all ACLs
#         cmd = [
#             "bash", kafka_acls_path,
#             "--bootstrap-server", bootstrap_server,
#             "--list",
#             "--command-config", "/path/to/client.properties"  # if using SASL/SSL auth
#         ]

#         result = subprocess.run(cmd, capture_output=True, text=True, check=True)
#         output = result.stdout.strip()

#         # Optional: parse output into structured JSON
#         # CLI output is usually in lines like:
#         # "User:alice has Allow permission on Topic:mytopic from host *"
#         acl_list = []
#         for line in output.splitlines():
#             parts = line.split()
#             if len(parts) >= 8:
#                 acl_list.append({
#                     "principal": parts[1],
#                     "permission": parts[4],
#                     "resource_type": parts[6].split(":")[0],
#                     "resource_name": parts[6].split(":")[1] if ":" in parts[6] else "",
#                     "host": parts[-1],
#                     "operation": parts[3],
#                 })

#         return JsonResponse({"acls": acl_list})

#     except subprocess.CalledProcessError as e:
#         print("Error running kafka-acls.sh:", e.stderr)
#         return JsonResponse({"error": e.stderr}, status=500)
#     except Exception as e:
#         print("list_acls_api error:", e)
#         return JsonResponse({"error": str(e)}, status=500)
    
@csrf_exempt
def create_acl_api(request):
    """
    POST: Create a Kafka ACL
    Expected JSON payload:
    {
        "principal": "uid from LDAP",
        "resource_type": "TOPIC|GROUP|CLUSTER",
        "resource_name": "topic_name or group_name",
        "permission": "READ|WRITE|ALL|DESCRIBE|CREATE|DELETE"
    }
    """
    if request.method != "POST":
        return JsonResponse({"error": "POST method required"}, status=405)

    try:
        data = json.loads(request.body)
        principal = data.get("principal")
        resource_type = data.get("resource_type")
        resource_name = data.get("resource_name") or ""
        permission = data.get("permission")

        # Validation
        if not principal or not resource_type or not permission:
            return JsonResponse({"error": "Missing required fields"}, status=400)

        # Ensure proper Kafka principal format
        if not principal.startswith("User:"):
            principal = f"User:{principal}"

        admin_client = AdminClient(conf)

        # --- Resource Type Mapping ---
        resource_type_map = {
            "TOPIC": ResourceType.TOPIC,
            "GROUP": ResourceType.GROUP,
           "CLUSTER": getattr(ResourceType, "CLUSTER", ResourceType.BROKER),
            # "CLUSTER": ResourceType.CLUSTER,
        }

        if resource_type not in resource_type_map:
            return JsonResponse({"error": f"Invalid resource_type: {resource_type}"}, status=400)

        # --- ACL Binding Creation ---
        # acl = AclBinding(
        #     ResourcePattern(
        #         resource_type_map[resource_type],
        #         resource_name,
        #         "LITERAL"
        #     ),
        #     principal,
        #     "*",  # host wildcard
        #     getattr(AclOperation, permission),
        #     AclPermissionType.ALLOW
        # )
        acl = AclBinding(
            resource_type_map[resource_type],
            resource_name,
            "LITERAL",
            principal,
            "*",
            getattr(AclOperation, permission),
            AclPermissionType.ALLOW
        )


        # --- Send ACL Request ---
        fs = admin_client.create_acls([acl])

        for f in fs.values():
            f.result()  # Will raise exception if failed

        return JsonResponse({
            "status": "success",
            "message": f"ACL created for {principal} on {resource_type}:{resource_name}"
        })

    except Exception as e:
        logger.exception("Error creating ACL")
        return JsonResponse({"status": "error", "message": str(e)}, status=500)
    
# @csrf_exempt
# @login_required
# def create_acl_api(request):
#     """
#     POST: Create a Kafka ACL
#     Expected JSON payload:
#     {
#         "principal": "uid from LDAP",
#         "resource_type": "TOPIC|GROUP|CLUSTER",
#         "resource_name": "topic_name or group_name",
#         "permission": "READ|WRITE|ALL|..."
#     }
#     """
#     if request.method != "POST":
#         return JsonResponse({"error": "POST method required"}, status=405)

#     try:
#         data = json.loads(request.body)
#         principal = data.get("principal")
#         resource_type = data.get("resource_type")
#         resource_name = data.get("resource_name") or ""  # CLUSTER can be empty
#         permission = data.get("permission")

#         if not principal or not resource_type or not permission:
#             return JsonResponse({"error": "Missing required fields"}, status=400)

#         # Format principal as expected by Kafka (User:uid)
#         if not principal.startswith("User:"):
#             principal = f"User:{principal}"

#         # Call the utility function
#         result = create_acl(
#             principal=principal,
#             resource_type=resource_type,
#             resource_name=resource_name,
#             permission=permission
#         )

#         return JsonResponse(result)

#     except Exception as e:
#         logger.exception("Error creating ACL")
#         return JsonResponse({"status": "error", "message": str(e)}, status=500)
    
# @csrf_exempt
# @login_required
# @user_passes_test(is_admin)
# def create_acl_api(request):
#     """
#     POST JSON: {principal, resource_type, resource_name, permission}
#     Admin-only
#     """
#     try:
#         payload = json.loads(request.body.decode('utf-8'))
#         principal = payload.get('principal')
#         resource_type = payload.get('resource_type')
#         resource_name = payload.get('resource_name', '')
#         permission = payload.get('permission')

#         if not principal or not resource_type or not permission:
#             return JsonResponse({"error": "missing params"}, status=400)

#         # call Kafka Admin to create ACLs
#         errors = create_acl(principal=principal, resource_type=resource_type, resource_name=resource_name, permission=permission)

#         # store a record in DB (for UI convenience & auditing)
#         acl_obj = AclEntry.objects.create(
#             resource_type=resource_type,
#             resource_name=resource_name,
#             principal=principal,
#             permission=permission,
#             granted_by=request.user
#         )

#         if errors:
#             return JsonResponse({"status": "partial_error", "errors": errors}, status=207)
#         return JsonResponse({"status": "success", "acl_id": acl_obj.id})
#     except Exception as e:
#         logger.exception("create_acl_api")
#         return JsonResponse({"error": str(e)}, status=500)


@csrf_exempt
@login_required
@user_passes_test(is_admin)
def delete_acl_api(request):
    """
    POST JSON: {principal, resource_type, resource_name, operation(optional)}
    Admin-only
    """
    try:
        payload = json.loads(request.body.decode('utf-8'))
        principal = payload.get('principal')
        resource_type = payload.get('resource_type')
        resource_name = payload.get('resource_name', '')
        operation = payload.get('operation')  # optional

        # call kafka_utils.delete_acl
        errors = delete_acl(principal=principal, resource_type=resource_type, resource_name=resource_name, operation=operation)

        # remove DB entries matching
        AclEntry.objects.filter(principal=principal, resource_type=resource_type, resource_name=resource_name).delete()

        if errors:
            return JsonResponse({"status": "partial_error", "errors": errors}, status=207)
        return JsonResponse({"status": "success"})
    except Exception as e:
        logger.exception("delete_acl_api")
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def list_topics_api(request):
    """
    GET: returns a list of topics from Kafka via AdminClient
    """
    try:
        client = __import__('confluent_kafka.admin', fromlist=['']).AdminClient(settings.KAFKA_CLIENT_CONFIG)
        md = client.list_topics(timeout=10.0)
        topics = [t for t in md.topics.keys()]
        return JsonResponse({"topics": topics})
    except Exception as e:
        logger.exception("list_topics_api")
        return JsonResponse({"error": str(e)}, status=500)

@login_required
def list_ldap_users_api(request):
    """
    GET: List LDAP users correctly
    """
    LDAP_SERVER = getattr(settings, 'AUTH_LDAP_SERVER_URI', None)
    LDAP_BIND_DN = getattr(settings, 'AUTH_LDAP_BIND_DN', None)
    LDAP_BIND_PASSWORD = getattr(settings, 'AUTH_LDAP_BIND_PASSWORD', None)
    LDAP_USER_BASE = getattr(settings, 'AUTH_LDAP_USER_BASE', None)

    LDAP_USER_FILTER = "(|(objectClass=posixaccount)(objectClass=inetOrgPerson))"

    try:
        logger.info(f"[LDAP DEBUG] LDAP_SERVER={LDAP_SERVER}")
        logger.info(f"[LDAP DEBUG] LDAP_BIND_DN={LDAP_BIND_DN}")
        logger.info(f"[LDAP DEBUG] LDAP_USER_BASE={LDAP_USER_BASE}")

        if LDAP_SERVER and LDAP_BIND_DN and LDAP_BIND_PASSWORD and LDAP_USER_BASE:
            server = Server(LDAP_SERVER, get_info=ALL)
            conn = Connection(server, LDAP_BIND_DN, LDAP_BIND_PASSWORD, auto_bind=True)
            logger.info("[LDAP DEBUG] Connected successfully")

            conn.search(
                search_base=LDAP_USER_BASE,
                search_filter=LDAP_USER_FILTER,
                search_scope=SUBTREE,
                attributes=['uid', 'cn', 'mail', 'dn']
            )

            logger.info(f"[LDAP DEBUG] Found {len(conn.entries)} entries in LDAP search")

            users = [
                {
                    "uid": str(entry.uid) if 'uid' in entry else None,
                    "cn": str(entry.cn) if 'cn' in entry else None,
                    "mail": str(entry.mail) if 'mail' in entry else None,
                    "dn": str(entry.entry_dn),
                }
                for entry in conn.entries
            ]
            conn.unbind()

            return JsonResponse({"users": users})
        else:
            logger.warning("[LDAP DEBUG] Missing LDAP settings, using Django fallback")
            users = [{"username": u.username, "id": u.id} for u in User.objects.all()[:200]]
            return JsonResponse({"users": users})

    except Exception as e:
        logger.exception("[LDAP DEBUG] LDAP fetch failed")
        return JsonResponse({"error": str(e)}, status=500)



@csrf_exempt
def create_topic_api(request, request_id):
    """
    Allows a normal authenticated user to create a Kafka topic
    for their approved topic request.
    """
    if request.method != "POST":
        return JsonResponse(
            {"success": False, "message": "Invalid request method. Use POST."},
            status=400
        )

    user = request.user
    if not user.is_authenticated:
        return JsonResponse({"success": False, "message": "Unauthorized"}, status=401)

    try:
        # Only allow normal (non-admin) users
        if user.is_superuser:
            return JsonResponse({"success": False, "message": "Admins cannot use this endpoint."}, status=403)

        # Fetch approved request made by this user
        topic_request = TopicRequest.objects.get(
            id=request_id,
            requested_by=user,
            status='APPROVED'
        )

        topic_name = topic_request.topic_name
        partitions = topic_request.partitions

        #  Create Kafka topic
        # admin_client = AdminClient({'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)})
        admin_client = AdminClient(conf);

        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=partitions,
            replication_factor=1
        )

        try:
            fs = admin_client.create_topics([new_topic])
            for topic, f in fs.items():
                f.result()  # will raise exception if Kafka fails

        except Exception as kafka_error:
            logger.error(f"[{user.username}] Kafka topic creation error: {kafka_error}")
            return JsonResponse(
                {"success": False, "message": f"Kafka error: {kafka_error}"},
                status=500
            )

        # Save to DB
        created_topic = Topic.objects.create(
            name=topic_name,
            partitions=partitions,
            created_by=user,
            production="Active",
            consumption="Active",
            followers=1,
            observers=0,
            last_produced=timezone.now()
        )

        # Topic.objects.create(
        #     name=topic_name,
        #     partitions=partitions,
        #     created_by=user,
        #     production="Active",
        #     consumption="Active",
        #     followers=1,
        #     observers=0,
        #     last_produced=timezone.now()
        # )

        # Update request status
        topic_request.status = 'PROCESSED'
        topic_request.save()
        # --- NEW ---
        broadcast_to_admin({
            "event": "topic_created",
            "topic_name": created_topic.name,
            "partitions": created_topic.partitions,
            "created_by": request.user.username
        })

        # broadcast_to_users({"type": "user_refresh"})
        broadcast_to_users({"event": "user_refresh"})

        return JsonResponse(
            {"success": True, "message": f"Topic '{topic_name}' created successfully."}
        )

    except TopicRequest.DoesNotExist:
        return JsonResponse(
            {"success": False, "message": "Approved request not found or unauthorized."},
            status=404
        )

    except Exception as e:
        logger.error(f"[{user.username}] Unexpected error: {e}")
        return JsonResponse(
            {"success": False, "message": f"Unexpected error: {str(e)}"},
            status=500
        )

@login_required
def create_topic_form(request, request_id):
    try:
        topic_request = TopicRequest.objects.get(id=request_id, requested_by=request.user, status='APPROVED')
        highlight = request.GET.get('highlight', '')
        context = {
            "topic_name": topic_request.topic_name,
            "partitions": topic_request.partitions,
            "username": request.user.username,
            "topics": Topic.objects.filter(created_by=request.user, is_active=True),
            "request_id": request_id,
            "highlight": highlight
        }
        if request.method == "GET":
            return render(request, "create_topic.html", context)
        elif request.method == "POST":
            return create_topic(request)
    except TopicRequest.DoesNotExist:
        return render(request, "home.html", {
            "topics": Topic.objects.filter(is_active=True, created_by=request.user),
            "username": request.user.username,
            "error": "Approved request not found or you don't have permission."
        })
                         
@csrf_exempt
def create_topic(request):
    if not request.user.is_authenticated:
        return redirect("login")
    if request.method == "POST":
        topic_name = request.POST.get("topic_name").strip()
        partitions = request.POST.get("partitions")
        request_id = request.POST.get("request_id")
        if topic_name and partitions:
            try:
                partitions = int(partitions)
                if partitions < 1:
                    return render(request, "create_topic.html", {
                        "topic_name": topic_name,
                        "partitions": partitions,
                        "username": request.user.username,
                        "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                        "error": "Partitions must be at least 1."
                    })
                if not re.match(r'^[a-zA-Z0-9._-]+$', topic_name):
                    return render(request, "create_topic.html", {
                        "topic_name": topic_name,
                        "partitions": partitions,
                        "username": request.user.username,
                        "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                        "error": "Topic name can only contain letters, numbers, dots, underscores, and hyphens."
                    })
                if not request.user.is_superuser:
                    approved_request = TopicRequest.objects.filter(
                        requested_by=request.user,
                        topic_name=topic_name,
                        status='APPROVED'
                    ).first()
                    if not approved_request:
                        return render(request, "home.html", {
                            "topics": Topic.objects.filter(is_active=True, created_by=request.user),
                            "username": request.user.username,
                            "error": "You need superuser approval to create this topic."
                        })
                # Check for existing topic and handle accordingly
                existing_topic = Topic.objects.filter(name=topic_name, is_active=True).first()
                if existing_topic:
                    if approved_request:
                        approved_request.status = 'PROCESSED'
                        approved_request.save()
                        logger.info(f"Request for '{topic_name}' marked as PROCESSED due to existing topic")
                    messages.warning(request, f"Topic '{topic_name}' already exists. Request marked as processed.")
                    return redirect("home")
                try:
                    # admin_client = AdminClient({
                    #     'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)
                    # })
                    admin_client = AdminClient(conf);
                    new_topic = NewTopic(topic_name, num_partitions=partitions, replication_factor=1)
                    admin_client.create_topics([new_topic])
                    logger.info(f"Kafka topic '{topic_name}' created with {partitions} partitions")
                except Exception as e:
                    logger.error(f"Kafka topic creation failed: {e}")
                    return render(request, "create_topic.html", {
                        "topic_name": topic_name,
                        "partitions": partitions,
                        "username": request.user.username,
                        "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                        "error": f"Failed to create topic in Kafka: {str(e)}"
                    })
                Topic.objects.create(
                    name=topic_name,
                    partitions=partitions,
                    created_by=request.user,
                    production="Active",
                    consumption="Active",
                    followers=1,
                    observers=0,
                    last_produced=timezone.now()
                )
                if approved_request:
                    approved_request.status = 'PROCESSED'
                    approved_request.save()
                    logger.info(f"Request for '{topic_name}' marked as PROCESSED after creation")
                logger.info(f"Created topic '{topic_name}' in Django by {request.user.username}")
                messages.success(request, f"Topic '{topic_name}' created successfully!")
                return redirect("home")
            except ValueError:
                return render(request, "create_topic.html", {
                    "topic_name": topic_name,
                    "partitions": partitions,
                    "username": request.user.username,
                    "topics": Topic.objects.filter(created_by=request.user, is_active=True),
                    "error": "Invalid number of partitions."
                })
        return render(request, "create_topic.html", {
            "topic_name": topic_name,
            "partitions": partitions,
            "username": request.user.username,
            "topics": Topic.objects.filter(created_by=request.user, is_active=True),
            "error": "Please fill all fields."
        })
    return redirect("home")

@csrf_exempt
def alter_topic_partitions(request, topic_name):
    if request.method != "PATCH":
        return JsonResponse({"success": False, "message": "Invalid method"}, status=405)

    # 🚫 Block internal topics
    if topic_name.startswith("_"):
        return JsonResponse({
            "success": False,
            "message": "Internal topics cannot be altered"
        }, status=403)

    try:
        data = json.loads(request.body.decode("utf-8"))
        new_count = data.get("new_partition_count")

        if not isinstance(new_count, int):
            return JsonResponse({
                "success": False,
                "message": "new_partition_count must be an integer"
            }, status=400)

        admin_client = AdminClient(conf)
        md = admin_client.list_topics(timeout=10)

        if topic_name not in md.topics:
            return JsonResponse({
                "success": False,
                "message": "Topic not found in Kafka"
            }, status=404)

        current = len(md.topics[topic_name].partitions)

        if new_count <= current:
            return JsonResponse({
                "success": False,
                "message": f"New partition count must be greater than {current}"
            }, status=400)

        fs = admin_client.create_partitions([
            NewPartitions(topic_name, new_count)
        ])

        for _, f in fs.items():
            f.result()

        # update DB only if topic exists there
        Topic.objects.filter(name=topic_name).update(
            partitions=new_count
        )

        return JsonResponse({
            "success": True,
            "message": f"Partitions increased {current} → {new_count}"
        })

    except Exception as e:
        return JsonResponse({
            "success": False,
            "message": str(e)
        }, status=500)

# @csrf_exempt
# def alter_topic_partitions(request, topic_name):
#     if request.method != "PATCH":
#         return JsonResponse({"success": False, "message": "Invalid method"})

#     data = json.loads(request.body.decode("utf-8"))
#     new_count = data.get("new_partition_count")

#     admin_client = AdminClient(conf)
#     md = admin_client.list_topics(timeout=10)

#     if topic_name not in md.topics:
#         return JsonResponse({"success": False, "message": "Topic not found"})

#     current = len(md.topics[topic_name].partitions)

#     if new_count <= current:
#         return JsonResponse({
#             "success": False,
#             "message": f"Must be greater than {current}"
#         })

#     admin_client.create_partitions([
#         NewPartitions(topic_name, new_count)
#     ])

#     # update DB if exists (safe)
#     Topic.objects.filter(name=topic_name).update(partitions=new_count)

#     return JsonResponse({
#         "success": True,
#         "message": f"Partitions increased {current} → {new_count}"
#     })


# @csrf_exempt
# def alter_topic_partitions(request, topic_id):
#     if request.method != "PATCH":
#         return JsonResponse({"success": False, "message": "Invalid request method. Use PATCH."})

#     try:
#         topic = Topic.objects.get(id=topic_id)
#         data = json.loads(request.body.decode("utf-8"))
#         new_partition_count = data.get("new_partition_count")

#         if not new_partition_count or not isinstance(new_partition_count, int):
#             return JsonResponse({"success": False, "message": "Provide a valid integer for new_partition_count."})

#         # Connect to Kafka
#         # admin_client = AdminClient({
#         #     'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)
#         # })
#         admin_client = AdminClient(conf);
#         # Get current partitions from Kafka
#         metadata = admin_client.list_topics(timeout=10)
#         current_partitions = len(metadata.topics[topic.name].partitions)

#         logger.info(f"Current partitions for '{topic.name}': {current_partitions}")

#         if new_partition_count <= current_partitions:
#             return JsonResponse({
#                 "success": False,
#                 "message": f"Cannot reduce partitions. Current: {current_partitions}, Requested: {new_partition_count}"
#             })

#         new_parts = [
#              NewPartitions(topic=topic.name, new_total_count=new_partition_count)
#         ]
#         fs = admin_client.create_partitions(new_parts)

#         for t, f in fs.items():
#             try:
#                 f.result()  # Wait for operation completion
#                 logger.info(f"Topic '{t}' partition count increased to {new_partition_count}.")
#             except Exception as e:
#                 logger.error(f"Failed to alter partitions for {t}: {e}")
#                 return JsonResponse({"success": False, "message": f"Kafka alter failed: {str(e)}"})

#         # Update in database
#         topic.partitions = new_partition_count
#         topic.save()

#         return JsonResponse({
#             "success": True,
#             "message": f"Partitions for topic '{topic.name}' increased from {current_partitions} → {new_partition_count}"
#         })

#     except Topic.DoesNotExist:
#         return JsonResponse({"success": False, "message": "Topic not found."})
#     except Exception as e:
#         import traceback
#         logger.error(traceback.format_exc())
#         return JsonResponse({"success": False, "message": str(e)})

# @csrf_exempt
# def delete_topic_api(request, topic_id):
#     if request.method != "DELETE":
#         return JsonResponse({"success": False, "message": "Invalid request method."})

#     try:
#         topic = Topic.objects.get(id=topic_id)
#         logger.info(f"Received DELETE request for topic: {topic.name}")

#         # admin_client = AdminClient({
#         #     'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)
#         # })
#         admin_client = AdminClient(conf);
#         # Delete topic in Kafka
#         fs = admin_client.delete_topics([topic.name], operation_timeout=30)
#         for topic_name, f in fs.items():
#             try:
#                 f.result()  # Wait for operation to complete
#                 logger.info(f"Kafka topic '{topic_name}' deleted successfully.")
#             except Exception as e:
#                 logger.error(f"Kafka topic deletion failed: {e}")
#                 return JsonResponse({"success": False, "message": f"Kafka deletion failed: {str(e)}"})

#         # # Delete from DB
#         # topic.delete()
#         # # --- NEW ---
#         # broadcast_to_users({"type": "topic_deleted"})
#         # # broadcast_to_admin({"type": "admin_refresh"})
#         # broadcast_to_admin({
#         #     "event": "topic_deleted",
#         #     "topic_id": topic.id,
#         # })
#         # Save ID before deleting
#         topic_id_value = topic.id

#         topic.delete()

#         # Notify all users
#         broadcast_to_users({
#             "event": "topic_deleted",
#             "topic_id": topic_id_value,
#         })

#         # Notify all admins
#         broadcast_to_admin({
#             "event": "admin_refresh",
#         })


#         logger.info(f"Topic '{topic.name}' deleted successfully from Django DB.")
#         return JsonResponse({"success": True, "message": f"Topic '{topic.name}' deleted successfully!"})

#     except Topic.DoesNotExist:
#         return JsonResponse({"success": False, "message": "Topic not found."})
#     except Exception as e:
#         logger.error(f"Error deleting topic: {e}")
#         return JsonResponse({"success": False, "message": str(e)})

@csrf_exempt
def delete_topic_api(request, topic_id):
    if request.method != "DELETE":
        return JsonResponse({"success": False, "message": "Invalid request method."})

    try:
        topic = Topic.objects.get(id=topic_id)
        logger.info(f"Received DELETE request for topic: {topic.name}")

        admin_client = AdminClient(conf)
        fs = admin_client.delete_topics([topic.name], operation_timeout=30)

        for topic_name, f in fs.items():
            try:
                f.result()
                logger.info(f"Kafka topic '{topic_name}' deleted successfully.")
            except Exception as e:
                logger.error(f"Kafka topic deletion failed: {e}")
                return JsonResponse({"success": False, "message": f"Kafka deletion failed: {str(e)}"})

        # Save ID before deletion
        topic_id_value = topic.id
        topic_name_value = topic.name

        topic.delete()

        # WebSocket notifications
        broadcast_to_users({
            "event": "topic_deleted",
            "topic_id": topic_id_value,
            "topic_name": topic_name_value,
        })

        broadcast_to_admin({
            "event": "admin_refresh"
        })

        logger.info(f"Topic '{topic_name_value}' deleted successfully from Django DB.")
        return JsonResponse({"success": True, "message": f"Topic '{topic_name_value}' deleted successfully!"})

    except Topic.DoesNotExist:
        return JsonResponse({"success": False, "message": "Topic not found."})

    except Exception as e:
        logger.error(f"Error deleting topic: {e}")
        return JsonResponse({"success": False, "message": str(e)})



@csrf_exempt
def delete_topic(request):
    if request.method == "POST":
        topic_ids = request.POST.getlist("topic_ids")
        if not topic_ids:
            messages.error(request, "No topics selected for deletion.")
            return render(request, "create_topic.html", {
                "username": request.user.username,
                "topics": Topic.objects.filter(created_by=request.user, is_active=True)
            })
        try:
            # admin_client = AdminClient({
            #     'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP)
            # })
            admin_client = AdminClient(conf)
            for topic_id in topic_ids:
                topic = Topic.objects.get(id=topic_id, created_by=request.user, is_active=True)
                try:
                    admin_client.delete_topics([topic.name])
                    logger.info(f"Kafka topic '{topic.name}' deleted by {request.user.username}")
                except Exception as e:
                    logger.error(f"Kafka topic deletion failed: {e}")
                    messages.error(request, f"Failed to delete topic '{topic.name}' from Kafka: {str(e)}")
                    continue
                topic.delete()  # Permanent deletion
                logger.info(f"Topic '{topic.name}' permanently deleted in Django by {request.user.username}")
            messages.success(request, "Selected topics deleted successfully!")
            return redirect("home" if not request.user.is_superuser else "admin_dashboard")
        except Topic.DoesNotExist:
            messages.error(request, "One or more selected topics not found or you don't have permission.")
            return render(request, "create_topic.html", {
                "username": request.user.username,
                "topics": Topic.objects.filter(created_by=request.user, is_active=True)
            })
    return JsonResponse({"success": False, "message": "Invalid request"})

@csrf_exempt
def approved_topics_api(request):
    user = request.user

    if not user.is_authenticated:
        return JsonResponse({"success": False, "message": "User not authenticated."}, status=403)

    if request.method == "GET":
        # Only approved topics
        approved_topics = TopicRequest.objects.filter(status="APPROVED").order_by("topic_name")
        topic_list = [t.topic_name for t in approved_topics]

        return JsonResponse({"success": True, "topics": topic_list}, safe=False)

    return JsonResponse({"success": False, "message": "Unsupported request method."}, status=400)

@csrf_exempt
def history_api(request):
    user = request.user

    if not user.is_authenticated:
        return JsonResponse({"success": False, "message": "User not authenticated."}, status=403)

    if request.method == "GET":
        print("history_api called by:", user.username)

        # If admin, show all topic requests
        if user.is_superuser:
            topic_requests = TopicRequest.objects.all().order_by("-requested_at")
        else:
            topic_requests = TopicRequest.objects.filter(requested_by=user).order_by("-requested_at")

        history_data = [
            {
                "id": req.id,
                "topic_name": req.topic_name,
                "partitions": req.partitions,
                "status": req.status,
                "requested_by": req.requested_by.username,
                "requested_at": req.requested_at.strftime("%Y-%m-%d %H:%M:%S"),
            }
            for req in topic_requests
        ]

        return JsonResponse(
            {"success": True, "history": history_data, "role": "admin" if user.is_superuser else "user"},
            safe=False,
        )

    return JsonResponse({"success": False, "message": "Unsupported request method."}, status=400)

@login_required
def topic_detail(request, topic_name):
    try:
        topic = Topic.objects.get(name=topic_name, is_active=True, created_by=request.user)
        topic_request = TopicRequest.objects.filter(
            topic_name=topic_name,
            requested_by=request.user,
            status='APPROVED'
        ).order_by('-reviewed_at').first()
        request_id = topic_request.id if topic_request else None
        context = {
            "topic": topic,
            "username": request.user.username,
            "topics": Topic.objects.filter(created_by=request.user, is_active=True),
            "request_id": request_id
        }
        return render(request, "topic_detail.html", context)
    except Topic.DoesNotExist:
        return render(request, "topic_detail.html", {
            "topics": Topic.objects.filter(is_active=True, created_by=request.user),
            "username": request.user.username,
            "error": f"Topic {topic_name} does not exist or you don't have permission."
        })

def execute_confluent_command(command, topic_name=None, partitions=None):
    return False, "This is a placeholder function for confluent command"

@csrf_exempt
def topic_detail_api(request, topic_name):
    admin_client = AdminClient(conf)
    md = admin_client.list_topics(timeout=10)

    topic_md = md.topics.get(topic_name)

    if not topic_md:
        return JsonResponse(
            {"success": False, "message": "Topic not found"},
            status=404
        )

    is_internal = topic_name.startswith("_") or topic_name.startswith("__")

    # Try DB enrichment (optional)
    db_topic = Topic.objects.filter(name=topic_name, is_active=True).first()

    data = {
        "name": topic_name,
        "partitions": len(topic_md.partitions),
        "replication_factor": next(
            iter(topic_md.partitions.values())
        ).replicas.__len__(),
        "is_internal": is_internal,
        "created_by": (
            db_topic.created_by.username
            if db_topic and db_topic.created_by
            else "CLI / System"
        ),
    }

    return JsonResponse({"success": True, "topic": data})


# @csrf_exempt
# def topic_detail_api(request, topic_name):
#     try:
#         topic = Topic.objects.get(name=topic_name, is_active=True)
#         data = {
#             "id": topic.id,
#             "name": topic.name,
#             "partitions": topic.partitions,
#             "created_by": topic.created_by.username,
#             "production": topic.production,
#             "consumption": topic.consumption,
#             "followers": topic.followers,
#             "observers": topic.observers,
#             "last_produced": topic.last_produced,
#         }
#         return JsonResponse({"success": True, "topic": data})
#     except Topic.DoesNotExist:
#         return JsonResponse({"success": False, "message": "Topic not found"}, status=404)

# @login_required
# def create_partition(request, topic_name):
#     if not request.user.is_authenticated:
#         return redirect("login")
#     if request.method == "POST":
#         partitions_to_delete = request.POST.get("partitions")
#         if partitions_to_delete:
#             try:
#                 partitions_to_delete = int(partitions_to_delete)
#                 if partitions_to_delete < 1:
#                     return render(request, "topic_detail.html", {
#                         "topic": Topic.objects.get(name=topic_name, is_active=True, created_by=request.user),
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": "Partitions must be at least 1."
#                     })
#                 topic = Topic.objects.get(name=topic_name, is_active=True, created_by=request.user)
#                 if partitions_to_delete >= topic.partitions:
#                     return render(request, "topic_detail.html", {
#                         "topic": topic,
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": "Cannot delete more partitions than exist."
#                     })
#                 if topic.created_by != request.user and not request.user.is_superuser:
#                     return render(request, "topic_detail.html", {
#                         "topic": topic,
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": "You can only delete partitions from topics you created."
#                     })
#                 success, message = execute_confluent_command("delete_partition", topic_name, partitions_to_delete)
#                 if success:
#                     topic.partitions -= partitions_to_delete
#                     if topic.partitions <= 0:
#                         topic.delete()  # Permanently delete topic if no partitions remain
#                         logger.info(f"Topic '{topic_name}' permanently deleted due to no partitions by {request.user.username}")
#                     else:
#                         topic.save()
#                         LogEntry.objects.create(
#                             command=f"delete_partition_{topic_name}",
#                             approved=True,
#                             message=f"Permanently deleted {partitions_to_delete} partitions from {topic_name} by {request.user.username}"
#                         )
#                         logger.info(f"Permanently deleted {partitions_to_delete} partitions from {topic_name} by {request.user.username}")
#                     return redirect("home" if not request.user.is_superuser else "admin_dashboard")
#                 else:
#                     return render(request, "topic_detail.html", {
#                         "topic": topic,
#                         "username": request.user.username,
#                         "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                         "error": message
#                     })
#             except ValueError:
#                 return render(request, "topic_detail.html", {
#                     "topic": Topic.objects.get(name=topic_name, is_active=True, created_by=request.user),
#                     "username": request.user.username,
#                     "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#                     "error": "Invalid number of partitions."
#                 })
#             except Topic.DoesNotExist:
#                 return render(request, "topic_detail.html", {
#                     "topics": Topic.objects.filter(is_active=True, created_by=request.user),
#                     "username": request.user.username,
#                     "error": f"Topic {topic_name} does not exist or you don't have permission."
#                 })
#         return render(request, "topic_detail.html", {
#             "topic": Topic.objects.get(name=topic_name, is_active=True, created_by=request.user),
#             "username": request.user.username,
#             "topics": Topic.objects.filter(created_by=request.user, is_active=True),
#             "error": "Please specify the number of partitions."
#         })
#     return redirect("home" if not request.user.is_superuser else "admin_dashboard")

@login_required
def delete_partition(request, topic_name):
    # Redirect to topic_detail for consistency, where deletion is handled
    return redirect("topic_detail", topic_name=topic_name)

@login_required
def submit_request(request):
    if request.method == "POST":
        topic_name = request.POST.get("topic_name")
        partitions = request.POST.get("partitions")
        if topic_name and partitions:
            try:
                partitions = int(partitions)
                if partitions < 1:
                    return JsonResponse({"success": False, "message": "Partitions must be at least 1."})
                TopicRequest.objects.create(
                    topic_name=topic_name,
                    partitions=partitions,
                    requested_by=request.user
                )
                logger.info(f"Topic request for {topic_name} submitted by {request.user.username}")
                return JsonResponse({"success": True, "message": "Topic creation request submitted"})
            except ValueError:
                return JsonResponse({"success": False, "message": "Invalid number of partitions."})
        return JsonResponse({"success": False, "message": "Please fill all fields."})
    return JsonResponse({"success": False, "message": "Invalid request"})

# @csrf_exempt
# def approve_request(request, request_id):
#     if not request.user.is_authenticated or not request.user.is_superuser:
#         return redirect("home")
#     if request.method == "POST":
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')
#             topic_request.status = 'APPROVED'
#             topic_request.reviewed_by = request.user
#             topic_request.reviewed_at = timezone.now()
#             topic_request.save()
#             # --- NEW ---
#             # broadcast_to_users({"type": "request_approved", "topic_name": topic_request.topic_name})
#             broadcast_to_users({
#     "event": "request_approved",
#     "request_id": request_obj.id,
#     "topic_name": request_obj.topic_name,
#     "partitions": request_obj.partitions
# })

#             broadcast_to_admin({"type": "admin_refresh"})   
#             logger.info(f"Request {request_id} approved by {request.user.username}")
#             messages.success(request, f"Request for '{topic_request.topic_name}' approved!")
#         except TopicRequest.DoesNotExist:
#             messages.error(request, "Request not found or already processed.")
#         return redirect("admin_dashboard")
#     return redirect("admin_dashboard")

# @csrf_exempt
# def approve_request(request, request_id):

#     if not request.user.is_authenticated or not request.user.is_superuser:
#         return redirect("home")

#     if request.method == "POST":
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')
            
#             # Update DB
#             topic_request.status = 'APPROVED'
#             topic_request.reviewed_by = request.user
#             topic_request.reviewed_at = timezone.now()
#             topic_request.save()

#             # BROADCAST TO ALL USERS (so User Dashboard updates instantly)
#             broadcast_to_users({
#                 "event": "request_approved",
#                 "request_id": topic_request.id,
#                 "topic_name": topic_request.topic_name,
#                 "partitions": topic_request.partitions,
#             })

#             # BROADCAST TO ALL ADMINS (refresh admin dashboards)
#             broadcast_to_admin({
#                 "event": "admin_refresh"
#             })

#             logger.info(f"Request {request_id} approved by {request.user.username}")
#             messages.success(request, f"Request for '{topic_request.topic_name}' approved!")

#         except TopicRequest.DoesNotExist:
#             messages.error(request, "Request not found or already processed.")

#         return redirect("admin_dashboard")

#     return redirect("admin_dashboard")

# @csrf_exempt
# def approve_request(request, request_id):

#     if not request.user.is_authenticated or not request.user.is_superuser:
#         return redirect("home")

#     if request.method == "POST":
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')

#             # Update DB
#             topic_request.status = 'APPROVED'
#             topic_request.reviewed_by = request.user
#             topic_request.reviewed_at = timezone.now()
#             topic_request.save()

#             # Notify ONLY the requester (NOT all users)
#             broadcast_to_users({
#                 "event": "request_approved",
#                 "request_id": topic_request.id,
#                 "topic_name": topic_request.topic_name,
#                 "partitions": topic_request.partitions,
#                 "requested_by": topic_request.requested_by.username
#             })

#             # Refresh ALL user dashboards
#             broadcast_to_users({
#                 "event": "user_refresh"
#             })

#             # Refresh all admins
#             broadcast_to_admin({
#                 "event": "admin_refresh"
#             })

#             logger.info(f"Request {request_id} approved by {request.user.username}")
#             messages.success(request, f"Request for '{topic_request.topic_name}' approved!")

#         except TopicRequest.DoesNotExist:
#             messages.error(request, "Request not found or already processed.")

#         return redirect("admin_dashboard")

#     return redirect("admin_dashboard")

@csrf_exempt
def approve_request(request, request_id):

    if not request.user.is_authenticated or not request.user.is_superuser:
        return redirect("home")

    if request.method == "POST":
        try:
            topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')

            # Record admin review activity
            topic_request.reviewed_by = request.user
            topic_request.reviewed_at = timezone.now()

            # ---------------------------------------------
            # CASE 1: DELETE REQUEST
            # ---------------------------------------------
            if topic_request.request_type == "DELETE":

                topic_name = topic_request.topic_name

                # --- Kafka Delete ---
                admin_client = AdminClient(conf)
                fs = admin_client.delete_topics([topic_name], operation_timeout=30)
                for _, f in fs.items():
                    f.result()   # raises exception if failed

                # --- Mark topic inactive in DB ---
                Topic.objects.filter(name=topic_name).update(is_active=False)

                # Mark request as completed
                topic_request.status = "COMPLETED"
                topic_request.save()

                # --- Notify only the requesting user ---
                broadcast_to_users({
                    "event": "topic_deleted",
                    "topic_name": topic_name,
                    "requested_by": topic_request.requested_by.username
                })

                # --- Refresh all admins ---
                broadcast_to_admin({"event": "admin_refresh"})

                messages.success(request, f"Topic '{topic_name}' deleted successfully!")
                logger.info(f"DELETE request {request_id} approved by {request.user.username}")

                return redirect("admin_dashboard")

            # ---------------------------------------------
            # CASE 2: CREATE REQUEST
            # ---------------------------------------------
            elif topic_request.request_type == "CREATE":

                topic_request.status = "APPROVED"
                topic_request.save()

                # Notify ONLY the requester
                broadcast_to_users({
                    "event": "request_approved",
                    "request_id": topic_request.id,
                    "topic_name": topic_request.topic_name,
                    "partitions": topic_request.partitions,
                    "requested_by": topic_request.requested_by.username
                })

                # Refresh user dashboards
                broadcast_to_users({"event": "user_refresh"})

                # Refresh admin dashboards
                broadcast_to_admin({"event": "admin_refresh"})

                messages.success(request, f"Create request for '{topic_request.topic_name}' approved!")
                logger.info(f"CREATE request {request_id} approved by {request.user.username}")

                return redirect("admin_dashboard")

            else:
                messages.error(request, "Unknown request type.")
                return redirect("admin_dashboard")

        except TopicRequest.DoesNotExist:
            messages.error(request, "Request not found or already processed.")
            return redirect("admin_dashboard")

    return redirect("admin_dashboard")


# @csrf_exempt
# def decline_request(request, request_id):
#     if not request.user.is_authenticated or not request.user.is_superuser:
#         return redirect("home")
#     if request.method == "POST":
#         try:
#             topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')
#             topic_request.status = 'DECLINED'
#             topic_request.reviewed_by = request.user
#             topic_request.reviewed_at = timezone.now()
#             topic_request.save()
#             # --- NEW ---
#             # broadcast_to_users({"type": "request_declined"})
#             broadcast_to_users({
#                 "event": "request_declined",
#                 "request_id": topic_request.id,
#             })

#             broadcast_to_admin({"type": "admin_refresh"})
#             logger.info(f"Request {request_id} declined by {request.user.username}")
#             messages.success(request, f"Request for '{topic_request.topic_name}' declined.")
#         except TopicRequest.DoesNotExist:
#             messages.error(request, "Request not found or already processed.")
#         return redirect("admin_dashboard")
#     return redirect("admin_dashboard")

@csrf_exempt
def decline_request(request, request_id):

    if not request.user.is_authenticated or not request.user.is_superuser:
        return redirect("home")

    if request.method == "POST":
        try:
            topic_request = TopicRequest.objects.get(id=request_id, status='PENDING')

            topic_request.status = "DECLINED"
            topic_request.reviewed_by = request.user
            topic_request.reviewed_at = timezone.now()
            topic_request.save()

            # Notify only the requester
            broadcast_to_users({
                "event": "request_declined",
                "request_id": topic_request.id,
                "topic_name": topic_request.topic_name,
                "request_type": topic_request.request_type,
                "requested_by": topic_request.requested_by.username
            })

            # Refresh admin dashboard
            broadcast_to_admin({"event": "admin_refresh"})

            logger.info(
                f"{topic_request.request_type} request {request_id} "
                f"for topic '{topic_request.topic_name}' declined by {request.user.username}"
            )

            # Admin message
            if topic_request.request_type == "DELETE":
                messages.success(request, f"Delete request for '{topic_request.topic_name}' declined.")
            else:
                messages.success(request, f"Create request for '{topic_request.topic_name}' declined.")

        except TopicRequest.DoesNotExist:
            messages.error(request, "Request not found or already processed.")

        return redirect("admin_dashboard")

    return redirect("admin_dashboard")
