"""SQLAlchemy models for attestation requests, orders and received records."""

import enum
import uuid

from sqlalchemy import (BigInteger, Boolean, CheckConstraint, Column, DateTime,
                        Enum, ForeignKey, Index, Integer, String, UniqueConstraint,
                        func, text)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


def _new_uuid() -> str:
    return str(uuid.uuid4())


class OrderStatus(str, enum.Enum):
    """Workflow state of a single attestation order."""

    REJECTED = "rejected"
    PENDING_ORDER = "pending_order"
    ORDERED = "ordered"
    RECEIVED = "received"
    UNDER_REVIEW = "under_review"
    JOURNALIZED = "journalized"
    SENT_TO_REQUESTER = "sent_to_requester"


# native_enum=False gives a portable VARCHAR + CHECK on MSSQL, MySQL and PostgreSQL.
OrderStatusType = Enum(
    OrderStatus,
    name="order_status",
    native_enum=False,
    create_constraint=True,
    length=32,
    values_callable=lambda members: [member.value for member in members],
)


class RecordOutcome(str, enum.Enum):
    """How a received record was processed."""

    JOURNALIZED = "journalized"
    # The requesting sagsbehandler must journalize the record manually.
    SENT_TO_REQUESTER = "sent_to_requester"


RecordOutcomeType = Enum(
    RecordOutcome,
    name="record_outcome",
    native_enum=False,
    create_constraint=True,
    length=32,
    values_callable=lambda members: [member.value for member in members],
)


class NotificationType(str, enum.Enum):
    """Reason a notification is sent to the requester."""

    ORDER_REJECTION = "order_rejection"                 # Mail confirming that the order has been rejected.
    ORDER_CONFIRMATION = "order_confirmation"           # Mail confirming that the order was received.
    RECORD_JOURNALIZED = "record_journalized"           # Mail indicating that the record has been journalized.
    RECORD_ATTACHED = "record_attached"                 # Mail with the record attached for manual journalization.
    RECORD_UNDER_REVIEW = "record_under_review"         # Mail indicating that the record is under review by the legal team.
    RECORD_REQUIRES_REVIEW = "record_requires_review"   # Mail to legal team indicating that the record requires an additional review.


NotificationTypeType = Enum(
    NotificationType,
    name="notification_type",
    native_enum=False,
    create_constraint=True,
    length=32,
    values_callable=lambda members: [member.value for member in members],
)


class NotificationStatus(str, enum.Enum):
    """Delivery state of a notification."""

    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


NotificationStatusType = Enum(
    NotificationStatus,
    name="notification_status",
    native_enum=False,
    create_constraint=True,
    length=32,
    values_callable=lambda members: [member.value for member in members],
)


class SagsbehandlerRoles(str, enum.Enum):
    """Roles that are allowed to request attestations."""

    LEDER = "leder"
    STEDFORTRAEDER = "stedfortraeder for leder"


SagsbehandlerRoleType = Enum(
    SagsbehandlerRoles,
    name="sagsbehandler_role",
    native_enum=False,
    create_constraint=True,
    length=32,
    values_callable=lambda members: [member.value for member in members],
)


class AuthorizedSagsbehandler(Base):
    """Caseworker allowed to request attestations, loaded from the admin CSV."""

    __tablename__ = "authorized_sagsbehandler"
    dq_number = Column(String(16), primary_key=True)
    role = Column(SagsbehandlerRoleType, nullable=False)


class AttestationRequest(Base):
    """Submitted form: who requested attestations, for whom, and approval."""

    __tablename__ = "attestation_request"
    request_id = Column(String(36), primary_key=True, default=_new_uuid)
    requester_dq = Column(String(16), nullable=False)
    requester_name = Column(String(255), nullable=False)
    requester_email = Column(String(255), nullable=False)
    subject_cpr = Column(String(10), nullable=False)
    is_approved = Column(Boolean, nullable=False)
    requested_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    orders = relationship("AttestationOrder", back_populates="request", cascade="all, delete-orphan")


class AttestationOrder(Base):
    """Order for one attestation type within a request."""

    __tablename__ = "attestation_order"
    __table_args__ = (
        UniqueConstraint("request_id", "attest_type", name="uq_attestation_order_request_type"),
        Index(
            "ix_attestation_order_external_order_ref",
            "external_order_ref",
            unique=True,
            mssql_where=text("external_order_ref IS NOT NULL"),
        ),
    )
    order_id = Column(String(36), primary_key=True, default=_new_uuid)
    request_id = Column(String(36), ForeignKey("attestation_request.request_id", ondelete="CASCADE"), nullable=False, index=True)
    attest_type = Column(Integer, nullable=False)
    attest_subtype = Column(Integer)
    status = Column(OrderStatusType, nullable=False, index=True)
    external_order_ref = Column(String(255))
    ordered_at = Column(DateTime(timezone=True))
    request = relationship("AttestationRequest", back_populates="orders")
    received_record = relationship("ReceivedRecord", back_populates="order", uselist=False, passive_deletes="all")
    events = relationship("OrderEvent", back_populates="order", cascade="all, delete-orphan", order_by="OrderEvent.occurred_at")
    notifications = relationship("Notification", back_populates="order", cascade="all, delete-orphan")


class ReceivedRecord(Base):
    """Received criminal record and whether it was journalized or sent to the requester."""

    __tablename__ = "received_record"
    __table_args__ = (
        CheckConstraint(
            "(outcome IS NULL AND processed_at IS NULL)"
            " OR (outcome IS NOT NULL AND processed_at IS NOT NULL)",
            name="ck_received_record_outcome_processed_at",
        ),
    )
    record_id = Column(String(36), primary_key=True, default=_new_uuid)
    # Nullable so unmatched records can be stored and matched manually.
    order_id = Column(String(36), ForeignKey("attestation_order.order_id", ondelete="RESTRICT"), unique=True)
    is_clean = Column(Boolean, nullable=False)
    mail_uid = Column(String(36), nullable=False, unique=True)
    received_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    # NULL until the record has been processed.
    outcome = Column(RecordOutcomeType, index=True)
    processed_at = Column(DateTime(timezone=True))
    # SBSYS references are only set when outcome is journalized.
    sbsys_sags_nr = Column(String(50))
    sbsys_sags_id = Column(Integer)
    sbsys_document_id = Column(String(50))
    order = relationship("AttestationOrder", back_populates="received_record")


class OrderEvent(Base):
    """Append-only audit entry for an order status change."""

    __tablename__ = "order_event"
    event_id = Column(BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True)
    order_id = Column(String(36), ForeignKey("attestation_order.order_id", ondelete="CASCADE"), nullable=False, index=True)
    rejection_reason = Column(String(255))
    rejection_description = Column(String(255))
    status = Column(OrderStatusType, nullable=False)
    occurred_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    actor = Column(String(50), nullable=False)
    order = relationship("AttestationOrder", back_populates="events")


class Notification(Base):
    """Notification to the requester about an order, tracked separately from the order status."""

    __tablename__ = "notification"
    __table_args__ = (
        UniqueConstraint("order_id", "notification_type", name="uq_notification_order_type"),
    )
    notification_id = Column(String(36), primary_key=True, default=_new_uuid)
    order_id = Column(String(36), ForeignKey("attestation_order.order_id", ondelete="CASCADE"), nullable=False, index=True)
    notification_type = Column(NotificationTypeType, nullable=False)
    status = Column(NotificationStatusType, nullable=False, index=True, server_default=NotificationStatus.PENDING.value)
    attempts = Column(Integer, nullable=False, server_default=text("0"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    sent_at = Column(DateTime(timezone=True))
    # Must not contain personal data such as e-mail addresses or CPR numbers.
    last_error = Column(String(255))
    order = relationship("AttestationOrder", back_populates="notifications")