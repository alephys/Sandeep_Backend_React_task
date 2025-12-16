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

# Allow specific frontend origins
# CORS_ALLOWED_ORIGINS = [
#     "http://10.1.14.203",
#     # "http://10.1.14.203:5173",
#     # "http://localhost:5173",
#     # "http://127.0.0.1:5173",
# ]
CORS_ALLOWED_ORIGINS = [
    "http://10.1.14.203",
]

# Allow credentials (cookies / sessions)
CORS_ALLOW_CREDENTIALS = True

CORS_ALLOWED_ORIGIN_REGEXES = [
    # r"^https?://(10\.1\.14\.203|localhost):5173$,
    r"^https?://(127\.0\.0\.1|localhost):5173$",
]

# Optional (for development convenience)
CORS_ALLOW_HEADERS = [
    'content-type',
    'authorization',
    'x-csrftoken',
]

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
