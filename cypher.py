from cryptography.fernet import Fernet

encryption_key = b'w1GLAgxA5AK3DMcESVcdb166UcdZS4J31iIG0aNN8dw='

data = 'ale'
#data = 'MIBL$2023'

cipher_suite = Fernet(encryption_key)
encrypted_data = cipher_suite.encrypt(data.encode())
print(encrypted_data)