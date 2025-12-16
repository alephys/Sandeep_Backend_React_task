# myproject auth_backends.py

from ldap3 import Server, Connection, ALL, core
from django.contrib.auth.models import User
from django.conf import settings
from django.conf import settings

class LDAPBackend:
    def authenticate(self, request, username=None, password=None):
        ldap_server_url = "ldap://ldapnode.infra.alephys.com:389"
        user_dn_template = "uid={username},cn=users,cn=accounts,dc=alephys,dc=com"
        group_base = "cn=groups,cn=accounts,dc=alephys,dc=com"

        server = Server(ldap_server_url, get_info=ALL)

        # First, try to bind as the user (to verify credentials)
        try:
            user_dn = user_dn_template.format(username=username)
            conn = Connection(server, user=user_dn, password=password, auto_bind=True)
        ldap_server_url = "ldap://ldapnode.infra.alephys.com:389"
        user_dn_template = "uid={username},cn=users,cn=accounts,dc=alephys,dc=com"
        group_base = "cn=groups,cn=accounts,dc=alephys,dc=com"

        server = Server(ldap_server_url, get_info=ALL)

        # First, try to bind as the user (to verify credentials)
        try:
            user_dn = user_dn_template.format(username=username)
            conn = Connection(server, user=user_dn, password=password, auto_bind=True)
        except core.exceptions.LDAPException:
            # Invalid credentials
            return None

        if not conn.bound:
            # Invalid credentials
            return None

        if not conn.bound:
            return None

        # ✅ User authenticated successfully
        # Now, check if the user belongs to the 'admins' group
        is_admin = False
        try:
            # Search for groups where this user is a member
            conn.search(
                search_base=group_base,
                search_filter=f"(member={user_dn})",
                attributes=["cn"]
            )

            # Look for group names like 'admins' or 'admin'
            for entry in conn.entries:
                if "admin" in str(entry.cn).lower():
                    is_admin = True
                    break

        except Exception as e:
            print(f"LDAP group check failed: {e}")

        # Create or update the Django user
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            user = User.objects.create_user(username=username, password=password)

        # Update superuser/staff status based on LDAP group membership
        user.is_superuser = is_admin
        user.is_staff = is_admin
        user.save()

        return user
        # ✅ User authenticated successfully
        # Now, check if the user belongs to the 'admins' group
        is_admin = False
        try:
            # Search for groups where this user is a member
            conn.search(
                search_base=group_base,
                search_filter=f"(member={user_dn})",
                attributes=["cn"]
            )

            # Look for group names like 'admins' or 'admin'
            for entry in conn.entries:
                if "admin" in str(entry.cn).lower():
                    is_admin = True
                    break

        except Exception as e:
            print(f"LDAP group check failed: {e}")

        # Create or update the Django user
        try:
            user = User.objects.get(username=username)
        except User.DoesNotExist:
            user = User.objects.create_user(username=username, password=password)

        # Update superuser/staff status based on LDAP group membership
        user.is_superuser = is_admin
        user.is_staff = is_admin
        user.save()

        return user

    def get_user(self, user_id):
        try:
            return User.objects.get(pk=user_id)
        except User.DoesNotExist:
            return None
        
        
# from ldap3 import Server, Connection, ALL, core
# from django.contrib.auth.models import User

# class LDAPBackend:
#     def authenticate(self, request, username=None, password=None):
#         # server = Server("ldap://10.1.14.150:389", get_info=ALL)
#         server = Server("ldap://ldapnode.infra.alephys.com:389", get_info=ALL)

#         try:
#             conn = Connection(
#                 server,
#                 # f"uid={username},dc=example,dc=com",
#                 f"uid={username},cn=users,cn=accounts,dc=alephys,dc=com",
#                 password,
#                 auto_bind=True
#             )
#         except core.exceptions.LDAPException:
#             # Could not bind → return None so Django tries next backend
#             return None

#         if conn.bound:
#             # LDAP authentication successful
#             try:
#                 user = User.objects.get(username=username)
#             except User.DoesNotExist:
#                 user = User.objects.create_user(username=username, password=password)
#             return user

#         return None

#     def get_user(self, user_id):
#         try:
#             return User.objects.get(pk=user_id)
#         except User.DoesNotExist:
#             return None
