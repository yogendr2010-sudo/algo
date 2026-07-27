# ================================================================
# Payment Manager — Abstraction layer over payment providers.
#
# All payment requests pass through this manager. The system never
# directly depends on a specific payment provider. To add a new
# provider (Cashfree, Stripe, etc.), create a new provider class
# implementing the PaymentProvider protocol and register it in
# PaymentManager._init_providers().
#
# Admin can switch payment mode via BillingSettings.payment_mode
# without any code changes.
# ================================================================

from __future__ import annotations
from typing import Optional, Protocol, runtime_checkable

from backend.db.models import PaymentProvider as PaymentProviderEnum


@runtime_checkable
class PaymentProviderProtocol(Protocol):
    """Interface that every payment provider must implement."""
    
    @property
    def provider_name(self) -> str:
        ...

    def create_order(self, amount_rupees: float, receipt: str, 
                     notes: dict | None = None) -> dict:
        """
        Creates a payment order/request. Returns a dict with
        provider-specific fields needed by the frontend.
        """
        ...

    def verify_payment(self, payment: dict) -> bool:
        """
        Verifies a completed payment. Returns True if valid.
        """
        ...

    def process_webhook(self, raw_body: bytes, headers: dict) -> dict:
        """
        Processes incoming webhook from the payment provider.
        Returns a normalized dict with event_type and payment data.
        """
        ...


class PaymentManager:
    """
    Singleton-style manager that routes payment operations to the
    currently active provider based on BillingSettings.payment_mode.
    """

    def __init__(self):
        self._providers: dict[str, PaymentProviderProtocol] = {}
        self._init_providers()

    def _init_providers(self) -> None:
        """Register all available payment providers."""
        from backend.services.razorpay_service import RazorpayProvider
        from backend.services.manual_payment_service import ManualPaymentProvider

        razorpay = RazorpayProvider()
        manual = ManualPaymentProvider()

        self._providers[razorpay.provider_name] = razorpay
        self._providers[manual.provider_name] = manual

    def get_active_provider(self) -> PaymentProviderProtocol:
        """
        Returns the currently active payment provider based on
        BillingSettings. Reads from billing_cache to avoid a DB
        hit on every checkout.
        """
        from backend.services.billing_cache import get_billing_settings
        settings = get_billing_settings()
        mode = settings.get("payment_mode", PaymentProviderEnum.RAZORPAY.value)
        provider = self._providers.get(mode)
        if not provider:
            # Fallback to Razorpay if configured mode has no registered provider
            return self._providers[PaymentProviderEnum.RAZORPAY.value]
        return provider

    def get_active_mode(self) -> str:
        """Returns the active payment mode string (RAZORPAY or MANUAL)."""
        return self.get_active_provider().provider_name

    def is_manual_mode(self) -> bool:
        """Returns True if manual payment mode is active."""
        return self.get_active_mode() == PaymentProviderEnum.MANUAL.value

    def create_order(self, amount_rupees: float, receipt: str,
                     notes: dict | None = None) -> dict:
        """Routes order creation to the active provider."""
        return self.get_active_provider().create_order(amount_rupees, receipt, notes)

    def verify_payment(self, payment_data: dict) -> bool:
        """Routes payment verification to the active provider."""
        return self.get_active_provider().verify_payment(payment_data)


# Global instance
_manager: PaymentManager | None = None


def get_payment_manager() -> PaymentManager:
    global _manager
    if _manager is None:
        _manager = PaymentManager()
    return _manager

