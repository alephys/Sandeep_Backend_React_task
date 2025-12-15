# # # myproject/ldap_kafka_test.py
# # # from ldap3 import Server, Connection, ALL
# # # from accounts.ldap_config import LDAP_SERVER_URL, GROUP_BASE, USER_BASE, BIND_DN, BIND_PASSWORD

# # # server = Server(LDAP_SERVER_URL, get_info=ALL)
# # # conn = Connection(server, user=BIND_DN, password=BIND_PASSWORD, auto_bind=True)
# # # print("Connected to LDAP successfully!")

# # # conn.search(USER_BASE, "(objectClass=inetOrgPerson)", attributes=['uid', 'cn'])
# # # print("Users found with details:")
# # # for entry in conn.entries:
# # #     print(f"DN: {entry.entry_dn}, uid: {entry.uid}, cn: {entry.cn}")

# # # conn.search(GROUP_BASE, "(objectClass=posixGroup)", attributes=['cn', 'memberUid'])
# # # print("\nGroups found:")
# # # for entry in conn.entries:
# # #     print(f"Group: {entry.cn}, members: {entry.memberUid}")

# # # conn.unbind()

# # from ldap3 import Server, Connection, ALL, SUBTREE

# # LDAP_SERVER = "ldap://ldapnode.infra.alephys.com"
# # LDAP_BIND_DN = "uid=navya,cn=users,cn=accounts,dc=alephys,dc=com"
# # LDAP_PASSWORD = "4lph@123"
# # LDAP_USER_BASE = "cn=users,cn=accounts,dc=alephys,dc=com"
# # LDAP_FILTER = "(objectClass=posixaccount)"

# # server = Server(LDAP_SERVER, get_info=ALL)
# # conn = Connection(server, LDAP_BIND_DN, LDAP_PASSWORD, auto_bind=True)

# # conn.search(
# #     search_base=LDAP_USER_BASE,
# #     search_filter=LDAP_FILTER,
# #     search_scope=SUBTREE,
# #     attributes=['uid', 'cn']
# # )

# # for entry in conn.entries:
# #     print(entry)

# # conn.unbind()

# from ldap3 import Server, Connection, ALL, SUBTREE

# LDAP_SERVER = "ldap://ldapnode.infra.alephys.com"
# LDAP_BIND_DN = "uid=admin,cn=users,cn=accounts,dc=alephys,dc=com"
# LDAP_PASSWORD = "Alph@1234"
# LDAP_USER_BASE = "cn=users,cn=accounts,dc=alephys,dc=com"
# LDAP_FILTER = "(objectClass=posixaccount)"

# print("Connecting to LDAP server...")

# try:
#     server = Server(LDAP_SERVER, get_info=ALL)
#     conn = Connection(server, LDAP_BIND_DN, LDAP_PASSWORD, auto_bind=True)

#     print("✅ Successfully connected to LDAP server.")
#     print(f"Searching users in base DN: {LDAP_USER_BASE}")

#     conn.search(
#         search_base=LDAP_USER_BASE,
#         search_filter=LDAP_FILTER,
#         search_scope=SUBTREE,
#         attributes=['uid', 'cn', 'dn']
#     )

#     print(f"\nFound {len(conn.entries)} users:\n")
#     for entry in conn.entries:
#         print(f"- uid: {entry.uid if 'uid' in entry else 'N/A'}, dn: {entry.entry_dn}")

#     conn.unbind()

# except Exception as e:
#     print(f"❌ Error: {e}")

from ldap3 import Server, Connection, ALL, SUBTREE
from myproject import settings

def main():
    print("Connecting to LDAP server...")
    print(settings.LDAP_SERVER_URL)

    try:
        server = Server(settings.LDAP_SERVER_URL, get_info=ALL)
        conn = Connection(server, settings.BIND_DN, settings.BIND_PASSWORD, auto_bind=True)

        print("✅ Successfully connected to LDAP server.")
        print(f"Searching users in base DN: {settings.USER_BASE}")

        conn.search(
            search_base=settings.USER_BASE,
            search_filter=settings.LDAP_FILTER,
            search_scope=SUBTREE,
            attributes=['uid', 'cn', 'dn']
        )

        print(f"\nFound {len(conn.entries)} users:\n")
        for entry in conn.entries:
            print(f"- uid: {entry.uid if 'uid' in entry else 'N/A'}, dn: {entry.entry_dn}")

        conn.unbind()

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
