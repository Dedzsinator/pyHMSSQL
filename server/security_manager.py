"""Security Manager for pyHMSSQL Database Cluster.

This module implements comprehensive security features including:
- TLS/SSL encryption for all communications
- JWT-based authentication and authorization
- Role-based access control (RBAC)
- Network security policies
- Certificate management
- Audit logging
"""

import ssl
import jwt
import hashlib
import secrets
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
from pathlib import Path
import socket
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PrivateFormat,
    NoEncryption,
)


class SecurityLevel(Enum):
    """Security levels for different deployment environments"""

    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    HIGH_SECURITY = "high_security"


class UserRole(Enum):
    """User roles for RBAC"""

    ADMIN = "admin"
    DBA = "dba"
    DEVELOPER = "developer"
    ANALYST = "analyst"
    READ_ONLY = "read_only"
    SERVICE_ACCOUNT = "service_account"


class Permission(Enum):
    """Database permissions"""

    READ = "read"
    WRITE = "write"
    CREATE = "create"
    DROP = "drop"
    ALTER = "alter"
    INDEX = "index"
    BACKUP = "backup"
    RESTORE = "restore"
    ADMIN = "admin"
    REPLICATION = "replication"


class SecurityManager:
    """Comprehensive security manager for database cluster"""

    def __init__(
        self,
        security_level: SecurityLevel = SecurityLevel.PRODUCTION,
        cert_dir: str = "/certs",
        audit_log_dir: str = "/logs/audit",
    ):
        """Initialize security manager.

        Args:
            security_level: Security level for the deployment
            cert_dir: Directory for certificates
            audit_log_dir: Directory for audit logs
        """
        self.security_level = security_level
        self.cert_dir = Path(cert_dir)
        self.audit_log_dir = Path(audit_log_dir)

        # Create directories
        self.cert_dir.mkdir(parents=True, exist_ok=True)
        self.audit_log_dir.mkdir(parents=True, exist_ok=True)

        # Security configuration
        self.jwt_secret = self._generate_jwt_secret()
        self.jwt_algorithm = "HS256"
        self.token_expiry_hours = (
            24 if security_level != SecurityLevel.HIGH_SECURITY else 8
        )

        # User and permission management
        self.users: Dict[str, Dict[str, Any]] = {}
        self.role_permissions: Dict[UserRole, List[Permission]] = (
            self._init_role_permissions()
        )
        self.active_sessions: Dict[str, Dict[str, Any]] = {}

        # Network security
        self.allowed_ips: List[str] = []
        self.blocked_ips: List[str] = []
        self.rate_limits: Dict[str, Dict[str, Any]] = {}

        # SSL/TLS context
        self.ssl_context = None
        self._setup_ssl_context()

        # Audit logging
        self.logger = self._setup_audit_logger()

        # Security policies
        self.password_policy = self._init_password_policy()

        self.logger.info(
            f"SecurityManager initialized with {security_level.value} security level"
        )

    def _generate_jwt_secret(self) -> str:
        """Generate a secure JWT secret key"""
        secret_file = self.cert_dir / "jwt_secret.key"

        if secret_file.exists():
            return secret_file.read_text().strip()

        # Generate new secret
        secret = secrets.token_urlsafe(64)
        secret_file.write_text(secret)
        secret_file.chmod(0o600)  # Read-write for owner only

        return secret

    def _init_role_permissions(self) -> Dict[UserRole, List[Permission]]:
        """Initialize role-based permissions"""
        return {
            UserRole.ADMIN: list(Permission),  # All permissions
            UserRole.DBA: [
                Permission.READ,
                Permission.WRITE,
                Permission.CREATE,
                Permission.DROP,
                Permission.ALTER,
                Permission.INDEX,
                Permission.BACKUP,
                Permission.RESTORE,
                Permission.REPLICATION,
            ],
            UserRole.DEVELOPER: [
                Permission.READ,
                Permission.WRITE,
                Permission.CREATE,
                Permission.ALTER,
                Permission.INDEX,
            ],
            UserRole.ANALYST: [Permission.READ, Permission.INDEX],
            UserRole.READ_ONLY: [Permission.READ],
            UserRole.SERVICE_ACCOUNT: [
                Permission.READ,
                Permission.WRITE,
                Permission.REPLICATION,
            ],
        }

    def _init_password_policy(self) -> Dict[str, Any]:
        """Initialize password policy based on security level"""
        base_policy = {
            "min_length": 8,
            "require_uppercase": True,
            "require_lowercase": True,
            "require_digits": True,
            "require_special": True,
            "max_age_days": 90,
            "history_count": 5,
        }

        if self.security_level == SecurityLevel.HIGH_SECURITY:
            base_policy.update(
                {"min_length": 12, "max_age_days": 30, "history_count": 10}
            )
        elif self.security_level == SecurityLevel.DEVELOPMENT:
            base_policy.update(
                {"min_length": 6, "require_special": False, "max_age_days": 365}
            )

        return base_policy

    def _setup_ssl_context(self):
        """Setup SSL/TLS context for secure communications"""
        try:
            # Generate or load certificates
            cert_file, key_file = self._ensure_certificates()

            # Create SSL context
            self.ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self.ssl_context.load_cert_chain(cert_file, key_file)

            # Configure security level
            if self.security_level == SecurityLevel.HIGH_SECURITY:
                self.ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
                self.ssl_context.set_ciphers(
                    "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"
                )
            else:
                self.ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2

            # Verify client certificates in production
            if self.security_level in [
                SecurityLevel.PRODUCTION,
                SecurityLevel.HIGH_SECURITY,
            ]:
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED

            self.logger.info("SSL/TLS context configured successfully")

        except Exception as e:
            self.logger.error(f"Failed to setup SSL context: {e}")
            raise

    def _ensure_certificates(self) -> Tuple[str, str]:
        """Ensure SSL certificates exist, generate if needed"""
        cert_file = self.cert_dir / "server.crt"
        key_file = self.cert_dir / "server.key"
        ca_cert_file = self.cert_dir / "ca.crt"
        ca_key_file = self.cert_dir / "ca.key"

        # Generate CA certificate if not exists
        if not ca_cert_file.exists():
            self._generate_ca_certificate(ca_cert_file, ca_key_file)

        # Generate server certificate if not exists
        if not cert_file.exists():
            self._generate_server_certificate(
                cert_file, key_file, ca_cert_file, ca_key_file
            )

        return str(cert_file), str(key_file)

    def _generate_ca_certificate(self, ca_cert_file: Path, ca_key_file: Path):
        """Generate Certificate Authority certificate"""
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=(
                4096 if self.security_level == SecurityLevel.HIGH_SECURITY else 2048
            ),
        )

        # Create certificate
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "CA"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "pyHMSSQL"),
                x509.NameAttribute(NameOID.COMMON_NAME, "pyHMSSQL CA"),
            ]
        )

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.utcnow())
            .not_valid_after(datetime.utcnow() + timedelta(days=3650))  # 10 years
            .add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.DNSName("localhost"),
                        x509.DNSName("*.hmssql-cluster.svc.cluster.local"),
                    ]
                ),
                critical=False,
            )
            .add_extension(
                x509.BasicConstraints(ca=True, path_length=None),
                critical=True,
            )
            .add_extension(
                x509.KeyUsage(
                    key_cert_sign=True,
                    crl_sign=True,
                    digital_signature=False,
                    content_commitment=False,
                    key_encipherment=False,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .sign(private_key, hashes.SHA256())
        )

        # Write certificate and key
        with open(ca_cert_file, "wb") as f:
            f.write(cert.public_bytes(Encoding.PEM))

        with open(ca_key_file, "wb") as f:
            f.write(
                private_key.private_bytes(
                    Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
                )
            )

        # Set secure permissions
        ca_cert_file.chmod(0o644)
        ca_key_file.chmod(0o600)

        self.logger.info("Generated CA certificate")

    def _generate_server_certificate(
        self, cert_file: Path, key_file: Path, ca_cert_file: Path, ca_key_file: Path
    ):
        """Generate server certificate signed by CA"""
        # Load CA certificate and key
        with open(ca_cert_file, "rb") as f:
            ca_cert = x509.load_pem_x509_certificate(f.read())

        with open(ca_key_file, "rb") as f:
            ca_key = serialization.load_pem_private_key(f.read(), password=None)

        # Generate server private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=(
                4096 if self.security_level == SecurityLevel.HIGH_SECURITY else 2048
            ),
        )

        # Create server certificate
        subject = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "CA"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "pyHMSSQL"),
                x509.NameAttribute(NameOID.COMMON_NAME, "hmssql-server"),
            ]
        )

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_cert.subject)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.utcnow())
            .not_valid_after(datetime.utcnow() + timedelta(days=365))
            .add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.DNSName("localhost"),
                        x509.DNSName("hmssql-server"),
                        x509.DNSName("*.hmssql-cluster.svc.cluster.local"),
                        x509.DNSName(
                            "hmssql-cluster-0.hmssql-headless.hmssql-cluster.svc.cluster.local"
                        ),
                        x509.DNSName(
                            "hmssql-cluster-1.hmssql-headless.hmssql-cluster.svc.cluster.local"
                        ),
                        x509.DNSName(
                            "hmssql-cluster-2.hmssql-headless.hmssql-cluster.svc.cluster.local"
                        ),
                        x509.IPAddress(socket.inet_aton("127.0.0.1")),
                    ]
                ),
                critical=False,
            )
            .add_extension(
                x509.KeyUsage(
                    key_cert_sign=False,
                    crl_sign=False,
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(
                x509.ExtendedKeyUsage(
                    [
                        x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
                        x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                    ]
                ),
                critical=True,
            )
            .sign(ca_key, hashes.SHA256())
        )

        # Write certificate and key
        with open(cert_file, "wb") as f:
            f.write(cert.public_bytes(Encoding.PEM))

        with open(key_file, "wb") as f:
            f.write(
                private_key.private_bytes(
                    Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()
                )
            )

        # Set secure permissions
        cert_file.chmod(0o644)
        key_file.chmod(0o600)

        self.logger.info("Generated server certificate")

    def _setup_audit_logger(self) -> logging.Logger:
        """Setup audit logging"""
        logger = logging.getLogger("audit")
        logger.setLevel(logging.INFO)

        # File handler for audit logs
        audit_file = (
            self.audit_log_dir / f"audit_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler = logging.FileHandler(audit_file)
        file_handler.setLevel(logging.INFO)

        # JSON formatter for structured logging
        formatter = logging.Formatter(
            '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
            '"message": "%(message)s", "module": "%(name)s"}'
        )
        file_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        return logger

    def create_user(
        self,
        username: str,
        password: str,
        role: UserRole,
        email: str = None,
        metadata: Dict[str, Any] = None,
    ) -> bool:
        """Create a new user with specified role"""
        try:
            # Validate password
            if not self._validate_password(password):
                raise ValueError("Password does not meet policy requirements")

            # Check if user already exists
            if username in self.users:
                raise ValueError(f"User {username} already exists")

            # Hash password
            salt = secrets.token_hex(32)
            password_hash = self._hash_password(password, salt)

            # Create user record
            user_data = {
                "username": username,
                "password_hash": password_hash,
                "salt": salt,
                "role": role.value,
                "email": email,
                "created_at": datetime.utcnow().isoformat(),
                "last_login": None,
                "failed_attempts": 0,
                "locked": False,
                "password_history": [password_hash],
                "metadata": metadata or {},
            }

            self.users[username] = user_data

            self.logger.info(f"User created: {username} with role {role.value}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create user {username}: {e}")
            return False

    def authenticate_user(
        self, username: str, password: str, client_ip: str = None
    ) -> Optional[str]:
        """Authenticate user and return JWT token"""
        try:
            # Check IP restrictions
            if client_ip and not self._check_ip_allowed(client_ip):
                self.logger.warning(
                    f"Authentication attempt from blocked IP: {client_ip}"
                )
                return None

            # Check rate limiting
            if not self._check_rate_limit(username, client_ip):
                self.logger.warning(f"Rate limit exceeded for user: {username}")
                return None

            # Get user
            user = self.users.get(username)
            if not user:
                self.logger.warning(
                    f"Authentication failed: user not found: {username}"
                )
                return None

            # Check if account is locked
            if user.get("locked", False):
                self.logger.warning(
                    f"Authentication failed: account locked: {username}"
                )
                return None

            # Verify password
            if not self._verify_password(password, user["password_hash"], user["salt"]):
                # Increment failed attempts
                user["failed_attempts"] = user.get("failed_attempts", 0) + 1

                # Lock account after too many failures
                if user["failed_attempts"] >= 5:
                    user["locked"] = True
                    self.logger.warning(
                        f"Account locked due to failed attempts: {username}"
                    )

                self.logger.warning(
                    f"Authentication failed: invalid password: {username}"
                )
                return None

            # Reset failed attempts on successful login
            user["failed_attempts"] = 0
            user["last_login"] = datetime.utcnow().isoformat()

            # Generate JWT token
            token = self._generate_jwt_token(username, user["role"])

            # Store active session
            session_id = secrets.token_urlsafe(32)
            self.active_sessions[session_id] = {
                "username": username,
                "role": user["role"],
                "token": token,
                "created_at": datetime.utcnow().isoformat(),
                "client_ip": client_ip,
                "last_activity": datetime.utcnow().isoformat(),
            }

            self.logger.info(f"User authenticated successfully: {username}")
            return token

        except Exception as e:
            self.logger.error(f"Authentication error for user {username}: {e}")
            return None

    def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Validate JWT token and return user info"""
        try:
            payload = jwt.decode(
                token, self.jwt_secret, algorithms=[self.jwt_algorithm]
            )

            # Check if token is expired
            if payload.get("exp", 0) < time.time():
                return None

            return payload

        except jwt.InvalidTokenError:
            return None

    def check_permission(
        self, username: str, permission: Permission, resource: str = None
    ) -> bool:
        """Check if user has specific permission"""
        try:
            user = self.users.get(username)
            if not user:
                return False

            user_role = UserRole(user["role"])
            role_permissions = self.role_permissions.get(user_role, [])

            # Admin has all permissions
            if user_role == UserRole.ADMIN:
                return True

            # Check specific permission
            if permission in role_permissions:
                self.logger.info(
                    f"Permission granted: {username} {permission.value} on {resource}"
                )
                return True

            self.logger.warning(
                f"Permission denied: {username} {permission.value} on {resource}"
            )
            return False

        except Exception as e:
            self.logger.error(f"Permission check error: {e}")
            return False

    def audit_log(
        self,
        action: str,
        username: str = None,
        resource: str = None,
        details: Dict[str, Any] = None,
        success: bool = True,
    ):
        """Log audit event"""
        audit_entry = {
            "action": action,
            "username": username,
            "resource": resource,
            "success": success,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details or {},
        }

        self.logger.info(json.dumps(audit_entry))

    def _validate_password(self, password: str) -> bool:
        """Validate password against policy"""
        policy = self.password_policy

        if len(password) < policy["min_length"]:
            return False

        if policy["require_uppercase"] and not any(c.isupper() for c in password):
            return False

        if policy["require_lowercase"] and not any(c.islower() for c in password):
            return False

        if policy["require_digits"] and not any(c.isdigit() for c in password):
            return False

        if policy["require_special"] and not any(
            c in "!@#$%^&*()_+-=[]{}|;:,.<>?" for c in password
        ):
            return False

        return True

    def _hash_password(self, password: str, salt: str) -> str:
        """Hash password with salt"""
        return hashlib.pbkdf2_hex(password.encode(), salt.encode(), 100000)

    def _verify_password(self, password: str, password_hash: str, salt: str) -> bool:
        """Verify password against hash"""
        return self._hash_password(password, salt) == password_hash

    def _generate_jwt_token(self, username: str, role: str) -> str:
        """Generate JWT token for user"""
        payload = {
            "username": username,
            "role": role,
            "iat": time.time(),
            "exp": time.time() + (self.token_expiry_hours * 3600),
        }

        return jwt.encode(payload, self.jwt_secret, algorithm=self.jwt_algorithm)

    def _check_ip_allowed(self, client_ip: str) -> bool:
        """Check if IP is allowed"""
        if client_ip in self.blocked_ips:
            return False

        if self.allowed_ips and client_ip not in self.allowed_ips:
            return False

        return True

    def _check_rate_limit(self, username: str, client_ip: str = None) -> bool:
        """Check rate limiting"""
        current_time = time.time()
        key = f"{username}:{client_ip}" if client_ip else username

        if key not in self.rate_limits:
            self.rate_limits[key] = {"attempts": 1, "window_start": current_time}
            return True

        rate_data = self.rate_limits[key]

        # Reset window if expired (5 minute window)
        if current_time - rate_data["window_start"] > 300:
            rate_data["attempts"] = 1
            rate_data["window_start"] = current_time
            return True

        # Check if limit exceeded (10 attempts per 5 minutes)
        if rate_data["attempts"] >= 10:
            return False

        rate_data["attempts"] += 1
        return True

    def get_security_status(self) -> Dict[str, Any]:
        """Get security system status"""
        return {
            "security_level": self.security_level.value,
            "ssl_enabled": self.ssl_context is not None,
            "active_sessions": len(self.active_sessions),
            "total_users": len(self.users),
            "blocked_ips": len(self.blocked_ips),
            "rate_limited_entities": len(self.rate_limits),
            "certificate_expiry": self._get_certificate_expiry(),
            "password_policy": self.password_policy,
        }

    def _get_certificate_expiry(self) -> Optional[str]:
        """Get certificate expiry date"""
        try:
            cert_file = self.cert_dir / "server.crt"
            if cert_file.exists():
                with open(cert_file, "rb") as f:
                    cert = x509.load_pem_x509_certificate(f.read())
                return cert.not_valid_after.isoformat()
        except Exception:
            pass
        return None

    def cleanup_expired_sessions(self):
        """Clean up expired sessions"""
        current_time = datetime.utcnow()
        expired_sessions = []

        for session_id, session_data in self.active_sessions.items():
            created_at = datetime.fromisoformat(session_data["created_at"])
            if (current_time - created_at).total_seconds() > (
                self.token_expiry_hours * 3600
            ):
                expired_sessions.append(session_id)

        for session_id in expired_sessions:
            del self.active_sessions[session_id]

        if expired_sessions:
            self.logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
