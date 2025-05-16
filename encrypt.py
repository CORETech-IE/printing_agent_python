import json
import sys
from cryptography.fernet import Fernet

# Load the encryption key from config.json
def load_encryption_key(file_path="encrypt_config.JSON"):
    try:
        with open(file_path, "r") as file:
            config = json.load(file)
            return config["encryption_key"].encode()  # Convert to bytes
    except FileNotFoundError:
        print(f"Error: The configuration file '{file_path}' was not found.")
        sys.exit(1)
    except KeyError:
        print(f"Error: 'encryption_key' not found in the configuration file.")
        sys.exit(1)

# Main function
def main():
    # Check for input text as an argument
    if len(sys.argv) != 2:
        print("Usage: encrypt.exe <text_to_encrypt>")
        sys.exit(1)

    # Load the encryption key
    encryption_key = load_encryption_key()

    # Text to encrypt
    data = sys.argv[1]

    # Encrypt the data
    cipher_suite = Fernet(encryption_key)
    encrypted_data = cipher_suite.encrypt(data.encode())

    print(f"Encrypted data: {encrypted_data.decode()}")

# Entry point
if __name__ == "__main__":
    main()