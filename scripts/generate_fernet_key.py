import argparse
import sys
from typing import List

# Requires 'cryptography' library: pip install cryptography
from cryptography.fernet import Fernet
from loguru import logger

# Configure Loguru
logger.remove()
logger.add(sys.stderr, level="INFO")

def generate_fernet_key() -> str:
    """
    Generates a cryptographically secure Fernet key.

    Returns:
        str: The generated Fernet key, base64-encoded.
    """
    key_bytes = Fernet.generate_key()
    # Decode bytes to string for easier handling/display
    return key_bytes.decode('utf-8')

def main():
    """Handles command-line arguments for generating Fernet keys."""
    parser = argparse.ArgumentParser(
        description="Generate one or more secure Fernet keys.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-n", "--number",
        type=int,
        default=1,
        help="Number of Fernet keys to generate."
    )

    args = parser.parse_args()

    if args.number < 1:
        logger.error("Number of keys must be at least 1.")
        parser.error("Number of keys must be at least 1.")
        return

    logger.info(f"Generating {args.number} Fernet Key(s)...")
    generated_keys: List[str] = []
    for i in range(args.number):
        try:
            fernet_key = generate_fernet_key()
            generated_keys.append(fernet_key)
            # Log each key as it's generated
            logger.success(f"Key {i+1}: {fernet_key}")
        except Exception as e:
            # Catching generic Exception as Fernet generation is usually robust
            logger.error(f"Error generating key {i+1}: {e}")
            break # Stop generation if an error occurs

    # Optional: Could log all keys again at the end if needed,
    # but logging them individually above might be sufficient.

    if len(generated_keys) == args.number:
        logger.info("Generation complete.")
    else:
        logger.warning("Generation finished, but encountered errors.")


if __name__ == "__main__":
    main()