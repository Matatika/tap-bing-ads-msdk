"""BingAds Authentication."""

from __future__ import annotations

from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing_extensions import override


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class BingAdsAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for BingAds."""

    @override
    @property
    def oauth_request_body(self):
        return {
            "grant_type": "refresh_token",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "refresh_token": self.config["refresh_token"],
            "scope": self.oauth_scopes,
        }

    @classmethod
    def create_for_stream(cls, stream) -> BingAdsAuthenticator:
        """Instantiate an authenticator for a specific Singer stream.

        Args:
            stream: The Singer stream instance.

        Returns:
            A new authenticator.
        """
        return cls(
            stream=stream,
            auth_endpoint="https://login.microsoftonline.com/common/oauth2/v2.0/token",
            oauth_scopes="https://ads.microsoft.com/msads.manage",
        )
