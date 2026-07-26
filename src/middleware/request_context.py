import logging
import time
from uuid import uuid4

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

logger = logging.getLogger(__name__)


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Attach request IDs and emit duration-aware access logs."""

    async def dispatch(
        self,
        request: Request,
        callNext: RequestResponseEndpoint,
    ) -> Response:
        """Process one request with correlation context.

        Args:
            request: Incoming HTTP request.
            callNext: Next ASGI handler.

        Returns:
            The downstream response with an ``X-Request-ID`` header.

        Raises:
            Exception: Re-raises unhandled downstream errors.
        """
        try:
            requestId = request.headers.get("X-Request-ID") or str(uuid4())
            request.state.requestId = requestId
            startedAt = time.perf_counter()
            try:
                response = await callNext(request)
            except Exception:
                logger.exception(
                    "Request failed",
                    extra={
                        "requestId": requestId,
                        "method": request.method,
                        "path": request.url.path,
                    },
                )
                raise
            response.headers["X-Request-ID"] = requestId
            logger.info(
                "Request completed",
                extra={
                    "requestId": requestId,
                    "method": request.method,
                    "path": request.url.path,
                    "statusCode": response.status_code,
                    "durationMs": round((time.perf_counter() - startedAt) * 1000, 2),
                },
            )
            return response
        except Exception as e:  # pragma: no cover - diagnostic boundary
            print(f"Error in dispatch: {e}")
            raise
