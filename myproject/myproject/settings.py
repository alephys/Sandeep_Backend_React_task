"""
Django settings for myproject project.
"""

import os
from pathlib import Path
import ldap
from django_auth_ldap.config import LDAPSearch, GroupOfNamesType
import logging
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

SECRET_KEY = os.getenv("SECRET_KEY", "django-insecure-default-key")
DEBUG = True
ALLOWED_HOSTS = ['127.0.1.1', '127.0.0.1', 'localhost', '127.1.14.150']

ASGI_APPLICATION = "myproject.asgi.application"

KAFKA_CLI_PATH = r"C:\Kafka\kafka_2.13-3.9.1"   # your extracted Kafka folder

KAFKA_BOOTSTRAP_SERVER = "navyanode3.infra.alephys.com:9094"

KAFKA_CLIENT_PROPERTIES = r"C:\Kafka\client.properties"

INSTALLED_APPS = [
    "channels",
    "accounts",
    'corsheaders',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
]

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

CORS_ALLOWED_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]
CORS_ALLOW_CREDENTIALS = True
CORS_ALLOWED_ORIGIN_REGEXES = [r"^https?://(127\.0\.0\.1|localhost):5173$"]
CORS_ALLOW_HEADERS = ['content-type', 'authorization', 'x-csrftoken']

ROOT_URLCONF = 'myproject.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'myproject.wsgi.application'

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels_redis.core.RedisChannelLayer",
        "CONFIG": {
            "hosts": [("127.0.0.1", 6379)],
        },
    },
}


# --------------------------------------------------------
# Broker Configuration (loaded from .env)
# --------------------------------------------------------
# KAFKA_CLIENT_CONFIG = {
#     "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
#     "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
#     "ssl.ca.location": os.getenv("KAFKA_SSL_CA_LOCATION"),
#     "ssl.endpoint.identification.algorithm": os.getenv("KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM"),
# }

# --------------------------------------------------------
# LDAP Configuration (loaded from .env)
# --------------------------------------------------------
AUTH_LDAP_SERVER_URI = os.getenv("LDAP_SERVER_URL")
AUTH_LDAP_BIND_DN = os.getenv("BIND_DN")
AUTH_LDAP_BIND_PASSWORD = os.getenv("BIND_PASSWORD")
AUTH_LDAP_USER_BASE = os.getenv("USER_BASE")
AUTH_LDAP_GROUP_BASE = os.getenv("GROUP_BASE")
AUTH_LDAP_USER_FILTER = os.getenv("LDAP_FILTER", "(|(objectClass=posixaccount)(objectClass=inetOrgPerson))")
AUTH_LDAP_USER_NAME_ATTR = os.getenv("USER_NAME_ATTRIBUTE", "uid")
AUTH_LDAP_GROUP_NAME_ATTR = os.getenv("GROUP_NAME_ATTRIBUTE", "cn")

# Optional mapping for user flags
AUTH_LDAP_USER_FLAGS_BY_GROUP = {
    "is_superuser": f"cn=superusers,{AUTH_LDAP_GROUP_BASE}",
    "is_staff": f"cn=superusers,{AUTH_LDAP_GROUP_BASE}",
}

# Optional group type if you plan to use LDAP groups
AUTH_LDAP_GROUP_TYPE = GroupOfNamesType()

# Authentication backends
AUTHENTICATION_BACKENDS = (
    'django.contrib.auth.backends.ModelBackend',  # Fallback
    'myproject.auth_backends.LDAPBackend',   # Custom LDAP backend
)

# --------------------------------------------------------
# Database
# --------------------------------------------------------
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': BASE_DIR / 'db.sqlite3',
    }
}

# --------------------------------------------------------
# Password validation
# --------------------------------------------------------
AUTH_PASSWORD_VALIDATORS = [
    {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
    {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
    {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
    {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
]

# --------------------------------------------------------
# Internationalization
# --------------------------------------------------------
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Kolkata'
USE_TZ = True

# --------------------------------------------------------
# Static files
# --------------------------------------------------------
STATIC_URL = '/static/'
STATICFILES_DIRS = [os.path.join(BASE_DIR, 'static')]
STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')

DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# --------------------------------------------------------
# Logging
# --------------------------------------------------------
logger = logging.getLogger('django_auth_ldap')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


# """
# Django settings for myproject project.
# """

# import os
# from pathlib import Path
# import ldap
# from django_auth_ldap.config import LDAPSearch, GroupOfNamesType
# import logging
# from dotenv import load_dotenv
# load_dotenv()  # this reads values from your .env file


# BASE_DIR = Path(__file__).resolve().parent.parent

# SECRET_KEY = 'django-insecure-4z)z9zy!7fe^ef!p4pumaeq07g1q)v!@6ss4alf3e$xbm^l2hu'
# DEBUG = True  # Set to False in production
# ALLOWED_HOSTS = ['127.0.1.1', '127.0.0.1', 'localhost', '127.1.14.150']  # Add your domain in production

# INSTALLED_APPS = [
#     'corsheaders',
#     'django.contrib.admin',
#     'django.contrib.auth',
#     'django.contrib.contenttypes',
#     'django.contrib.sessions',
#     'django.contrib.messages',
#     'django.contrib.staticfiles',
#     'accounts',
# ]

# MIDDLEWARE = [
#     'corsheaders.middleware.CorsMiddleware',
#     'django.middleware.common.CommonMiddleware',
#     'django.middleware.security.SecurityMiddleware',
#     'django.contrib.sessions.middleware.SessionMiddleware',
#     'django.middleware.common.CommonMiddleware',
#     'django.middleware.csrf.CsrfViewMiddleware',
#     'django.contrib.auth.middleware.AuthenticationMiddleware',
#     'django.contrib.messages.middleware.MessageMiddleware',
#     'django.middleware.clickjacking.XFrameOptionsMiddleware',
# ]

# # Allow specific frontend origins
# CORS_ALLOWED_ORIGINS = [
#     "http://localhost:5173",
#     "http://127.0.0.1:5173",
# ]

# # Allow credentials (cookies / sessions)
# CORS_ALLOW_CREDENTIALS = True

# CORS_ALLOWED_ORIGIN_REGEXES = [
#     r"^https?://(127\.0\.0\.1|localhost):5173$",
# ]

# # Optional (for development convenience)
# CORS_ALLOW_HEADERS = [
#     'content-type',
#     'authorization',
#     'x-csrftoken',
# ]

# ROOT_URLCONF = 'myproject.urls'

# TEMPLATES = [
#     {
#         'BACKEND': 'django.template.backends.django.DjangoTemplates',
#         'DIRS': [os.path.join(BASE_DIR, 'templates')],
#         'APP_DIRS': True,
#         'OPTIONS': {
#             'context_processors': [
#                 'django.template.context_processors.request',
#                 'django.contrib.auth.context_processors.auth',
#                 'django.contrib.messages.context_processors.messages',
#             ],
#         },
#     },
# ]

# WSGI_APPLICATION = 'myproject.wsgi.application'

# # # LDAP Configuration
# # AUTH_LDAP_SERVER_URI = "ldap://127.0.0.1:389"
# # AUTH_LDAP_BIND_DN = "cn=admin,dc=confluentdemo,dc=io"
# # AUTH_LDAP_BIND_PASSWORD = "admin"
# # AUTH_LDAP_USER_SEARCH = LDAPSearch("ou=users,dc=confluentdemo,dc=io", ldap.SCOPE_SUBTREE, "(uid=%(user)s)")
# # AUTH_LDAP_GROUP_SEARCH = LDAPSearch("ou=groups,dc=confluentdemo,dc=io", ldap.SCOPE_SUBTREE, "(objectClass=posixGroup)")
# # AUTH_LDAP_GROUP_TYPE = GroupOfNamesType()
# # AUTH_LDAP_USER_ATTR_MAP = {
# #     "first_name": "givenName",
# #     "last_name": "sn",
# #     "email": "mail",
# # }
# # AUTH_LDAP_USER_FLAGS_BY_GROUP = {
# #     "is_superuser": "cn=superusers,ou=groups,dc=confluentdemo,dc=io",  # Map LDAP group to superuser
# #     "is_staff": "cn=superusers,ou=groups,dc=confluentdemo,dc=io",      # Allow admin access
# # }

# # AUTH_LDAP_SERVER_URI = os.getenv("LDAP_SERVER_URL")
# # AUTH_LDAP_BIND_DN = os.getenv("BIND_DN")
# # AUTH_LDAP_BIND_PASSWORD = os.getenv("BIND_PASSWORD")
# # LDAP_USER_BASE = os.getenv("USER_BASE")

# # AUTH_LDAP_USER_SEARCH = LDAPSearch(
# #     os.getenv("USER_BASE"),
# #     ldap.SCOPE_SUBTREE,
# #     f"({os.getenv('USER_NAME_ATTRIBUTE')}=%(user)s)"
# # )

# # AUTH_LDAP_GROUP_SEARCH = LDAPSearch(
# #     os.getenv("GROUP_BASE"),
# #     ldap.SCOPE_SUBTREE,
# #     f"({os.getenv('GROUP_NAME_ATTRIBUTE')}=%(user)s)"
# # )

# # AUTH_LDAP_GROUP_TYPE = GroupOfNamesType()

# # AUTH_LDAP_USER_FLAGS_BY_GROUP = {
# #     "is_superuser": f"cn=superusers,{os.getenv('GROUP_BASE')}",
# #     "is_staff": f"cn=superusers,{os.getenv('GROUP_BASE')}",
# # }

# # Testing
# LDAP_SERVER_URL = os.getenv("LDAP_SERVER_URL")
# BIND_DN = os.getenv("BIND_DN")
# BIND_PASSWORD = os.getenv("BIND_PASSWORD")
# USER_BASE = os.getenv("USER_BASE")
# LDAP_FILTER = os.getenv("LDAP_FILTER", "(objectClass=posixaccount)")

# AUTHENTICATION_BACKENDS = (
#     'myproject.auth_backends.LDAPBackend',
#     "django.contrib.auth.backends.ModelBackend",  # Fallback for local superusers
#     # "django_auth_ldap.backend.LDAPBackend",
# )

# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': BASE_DIR / 'db.sqlite3',
#     }
# }

# AUTH_PASSWORD_VALIDATORS = [
#     {'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator'},
#     {'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator'},
#     {'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator'},
#     {'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator'},
# ]

# LANGUAGE_CODE = 'en-us'
# TIME_ZONE = 'Asia/Kolkata'
# USE_TZ = True

# STATIC_URL = '/static/'
# STATICFILES_DIRS = [os.path.join(BASE_DIR, 'static')]
# STATIC_ROOT = os.path.join(BASE_DIR, 'staticfiles')  # For collectstatic in production

# DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

# # Logging configuration
# logger = logging.getLogger('django_auth_ldap')
# logger.addHandler(logging.StreamHandler())
# logger.setLevel(logging.DEBUG)