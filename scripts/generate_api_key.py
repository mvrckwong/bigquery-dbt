import secrets
import argparse
from typing import Optional
import sys

from loguru import logger

logger.remove()
logger.add(sys.stderr, level="INFO")

DEFAULT_NUM_BYTES = 50

def generate_api_key(num_bytes: int = DEFAULT_NUM_BYTES, prefix: Optional[str] = None) -> str:
    """
    Generates a cryptographically secure, URL-safe API key.

    Args:
        num_bytes (int): The number of random bytes to generate for the token part.
                         The resulting token length will be approximately ceil(num_bytes * 4 / 3).
                         Defaults to DEFAULT_NUM_BYTES (32 bytes => ~43 chars).
        prefix (Optional[str]): An optional prefix to prepend to the key (e.g., "sk_").
                                If None or empty, no prefix is added. Defaults to None.

    Returns:
        str: The generated API key.

    Raises:
        ValueError: If num_bytes is not a positive integer.
    """
    if not isinstance(num_bytes, int) or num_bytes <= 0:
        raise ValueError("Number of bytes (num_bytes) must be a positive integer.")

    random_token = secrets.token_urlsafe(num_bytes)

    if prefix:
        if not prefix.isalnum() and '_' not in prefix and '-' not in prefix:
            logger.warning(
                f"Prefix '{prefix}' contains characters other than alphanumerics, underscore, or hyphen."
            )
        return f"{prefix}{random_token}"
    else:
        return random_token


def main():
    """Handles command-line arguments for generating API keys."""
    parser = argparse.ArgumentParser(
        description="Generate one or more secure API keys.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-n", "--number",
        type=int,
        default=1,
        help="Number of API keys to generate."
    )
    parser.add_argument(
        "-l", "--length-bytes",
        type=int,
        default=DEFAULT_NUM_BYTES,
        dest='num_bytes',
        help=f"Number of random bytes for the key's token part. "
             f"More bytes = more security & longer key. "
             f"(e.g., 16 bytes -> ~22 chars, 24 bytes -> ~32 chars, 32 bytes -> ~43 chars)."
    )
    parser.add_argument(
        "-p", "--prefix",
        type=str,
        default=None,
        help="Optional prefix for the API key (e.g., 'sk_live_', 'pk_test_')."
    )

    args = parser.parse_args()

    if args.number < 1:
        logger.error("Number of keys must be at least 1.")
        parser.error("Number of keys must be at least 1.")
        return

    if args.num_bytes <= 0:
        logger.error("Length in bytes must be a positive integer.")
        parser.error("Length in bytes must be a positive integer.")
        return

    logger.info(f"Generating {args.number} API Key(s)...")
    for i in range(args.number):
        try:
            api_key = generate_api_key(num_bytes=args.num_bytes, prefix=args.prefix)
            logger.success(f"Key {i+1}: {api_key}")
        except ValueError as e:
            logger.error(f"Error generating key: {e}")
            break

    logger.info("Generation complete.")


if __name__ == "__main__":
    main()