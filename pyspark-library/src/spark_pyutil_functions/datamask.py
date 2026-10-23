# -*- coding: utf-8 -*-
"""

"""
import re
import os
import sys
import json
import time
import uuid
import boto3
import pyffx
import random
import base64
import secrets
import logging
import hashlib
import binascii
import datetime
from struct import pack
from jsonschema import validate
#from Cryptodome.PublicKey import RSA
#from Cryptodome.Cipher import PKCS1_OAEP
#from Cryptodome import Random
#from Cryptodome.Cipher import AES
#from Cryptodome.Cipher import Blowfish
#from Cryptodome.Util.Padding import pad, unpad
from base64 import b64encode, b64decode
from spark_pyutil.utility import get_and_validate_json

_logger = logging.getLogger(__name__) 

class DatamaskConfProcessError(Exception):
    pass

class DatamaskConfProcess:
    """DatamaskConfProcess Constructor
     This is the DatamaskConfProcess class to be used in datamask processes
    
     The constructor validate the parameter schema above and set private variables: domains, salts and job.
    
     :param config_path:
     :type config_path: list
     Path to the JSON parameter file
     :param jobName:
     Jobname to be searched inside the parameter file
     :type jobName: str
    """
 
    def __init__(self, config_path: list, jobName: str):
        """Constructor method"""
        self.__schema = {
         "title": "DatamaskConfProcess",
         "description": "List of udf parameters",
         "type": "array",
         "items": {
             "type": "object",
             "properties": {
                 "MaskFields": {
                     "type": "array",
                     "description": "List of Mask Field properties",
                     "items": {
                         "type": "object",
                         "properties": {
                             "Active": {"type": "boolean",
                                        "description": "If False turn off the mask field"},
                             "MaskFieldName": {"type": "string", "description": "Mask field name"},
                             "FieldName": {"type": "string", "description": "Filed Name"},
                             "FormatRE": {"type": "string",
                                          "description": "Regular expression RE to be procced. Use this expression to use just a field part. Default valeu '*.'"},
                             "DomainName": {"type": "string",
                                            "description": "Domain name to get mask configurations"},
                             "KeepMaskName": {"type": "boolean",
                                              "description": "If True process will keep the mask name, otherwise the mask field will renamed to the original field name"}
                         },
                         "additionalProperties": False,
                         "required": ["MaskFieldName", "FieldName", "DomainName"]
                     }
                 },
                 "ReverseDatasetPath": {"type": "string", "description": "ReverseDatasetPath"},
                 "DomainsPath": {"type": "string", "description": "The Domains path"},
                 "SaltsPath": {"type": "string", "description": "The salt path"}
                }
             },
            "additionalProperties": False,
            "required": ["MaskFields","DomainsPath","SaltsPath" ]
        }
        
        validate(instance=config_path, schema=self.__schema)
        config = config_path[0]
        
        self.__job = jobName
        if "ReverseDatasetPath" in config:
            self.__reverse_dataset_path = config["ReverseDatasetPath"]
        else:
            self.__reverse_dataset_path = None
        self.__set_mask_fields(config["MaskFields"])
        self.__domains = []
        self.__domains_idx = {}
        self.__salt = []
        self.__salt_idx = {}
        if len(self.__mask_fields) == 0:
            self.__partition_keys = []
            return
        self.__set_domains(config["DomainsPath"])
        self.__set_salts(config["SaltsPath"])
        self.__partition_keys = []
        return
 
    def __set_domains(self, config: str):
        """ Set domains and generate a index to fast get

        :param config:
        :type config: str
        """
        domains_schema = {
            "Domains": {
               	 "type": "array",
               	 "description": "List of data mask domains",
               	 "items": {
               		 "type": "object",
               		 "properties": {
               			 "DomainName": {"type": "string", "description": "Domain name"},
               			 "SaltName": {"type": "string",
               						  "description": "The sal name to be searched inside the sal file"},
               			 "Layout": {"type": "string",
               						"description": "Layout to be used to replace the salt and field. $SALT will be replaced by the sal value and $VALUE will be replaced by the field value"},
               			 "MaskType": {"type": "string", "description": "Algorithm type to mask the field"},
               			 "EncryptKey": {"type": "string",
               							"description": "Encryption key for Algorithm, only when you use aes, bowfish and fte "},
               			 "RSAPublicKey": {"type": "string",
               							  "description": "Path to a public key for Algorithm, only when you use RSA "},
               			 "RSAEncryptedPrivateKey": {"type": "string",
               										"description": "Path to an encrypted private key for Algorithm, only when you use RSA "},
               			 "RSACMKeyId": {"type": "string",
               							"description": "Customer Managed KMS key id for a private key decryption, only when you use RSA "},
               			 "AWSRegion": {"type": "string",
               						   "description": "AWS Region where Customer Managed KMS key exists. Using for RSA decryption "},
               			 "FieldAlias": {"type": "string",
               							"description": "Field alias to be used in the Reverse dataset"},
               			 "MaskFieldAlias": {"type": "string",
               								"description": "Mask field alias to be used in the reverse dataset"},
               			 "ReverseDataset": {
               				 "type": "object",
               				 "properties": {
               					 "Active": {"type": "boolean",
               								"description": "If False tur off the creation of the reverse dataset"},
               					 "DatasetPath": {"type": "string", "description": "Reverse dataset path"},
               					 "BackupDatasetPath": {"type": "string",
               										   "description": "Reverse backup dataset path"},
               					 "BackupDatasetPartitionLabels": {
               						 "type": "object",
               						 "description": "Backup Dataset Partition Labels",
               						 "properties": {
               							 "Year": {"type": "string",
               									  "description": "Backup Dataset Partition year Label"},
               							 "Month": {"type": "string",
               									   "description": "Backup Dataset Partition month Label"},
               							 "Day": {"type": "string",
               									 "description": "Backup Dataset Partition day Label"},
               							 "Hour": {"type": "string",
               									  "description": "Backup Dataset Partition hour Label"},
               							 "Id": {"type": "string", "description": "Backup Dataset Partition id Label"}
               						 }
               					 },
               					 "BackupCoalesce": {"type": "number",
               										"description": "If set the process will coalesce partitions before write in the reverse dataset. This is usually the number of files to be write in the ouput path"},
               					 "Coalesce": {"type": "number",
               								  "description": "If set the process will coalesce partitions before write in the reverse dataset. This is usually the number of files to be write in the ouput path"},
               					 "KeepOnlyBackup": {"type": "boolean",
               										"description": "If True only the backup table will be avaiable, Dafault False"},
               					 "JobNamePartition": {"type": "boolean",
               										  "description": "If set it will enable TableName Partition, Default False"},
               					 "JobNamePartitionFieldName": {"type": "string",
               												   "description": "Change the default field Name, Default 'JobName'"},
               					 "HashBucketing": {"type": "boolean",
               									   "description": "If set the process will create bucketing in partitions"},
               					 "ReverseTable": {"type": "string",
               									  "description": "If set the process will try to update the table metadata"},
               					 "ReverseDatabase": {"type": "string",
               										 "description": "If set the process will try to update the metadata, it needs to be set with  ReverseTable"},
               				 },
               				 "additionalProperties": False,
               				 "required": ["Active"]
               			 }
               		 },
               		 "additionalProperties": False,
               		 "required": ["DomainName", "Layout", "MaskType", "FieldAlias", "MaskFieldAlias"]
                }   
            }
        }

        domains_idx = {}
        config_json = get_and_validate_json(config, domains_schema)
        domains = config_json["Domains"]
        
        idx = 0
        for idx, d in enumerate(domains):
            domains_idx[d["DomainName"]] = idx
        self.__domains = domains
        self.__domains_idx = domains_idx
        return

    def __set_salts(self, config: str):
        """ Validate the Salt file, parse the JSON file and generate a index to fast get.

        :param config:
        :type config: str
        """
        salt_schema = {
            "tittle": "Salf Json File",
            "type": "array",
            "description": "File to store the salts",
            "items": {
                "type": "object",
                "properties": {
                    "SaltName": {"type": "string", "description": "Salt name"},
                    "SaltValue": {"type": "string", "description": "Salt Value"}
                },
                "additionalProperties": False,
                "required": ["SaltName", "SaltValue"]
            }
        }
        salt_path = config
        
        salt = get_and_validate_json(salt_path, salt_schema)
        salt_idx = {}

        idx = 0
        for s in salt:
            salt_idx[salt[idx]["SaltName"]] = idx
            idx = idx + 1
        self.__salt = salt
        self.__salt_idx = salt_idx
        return
    
    def __set_mask_fields(self, config: str):
        self.__mask_fields = config

    def __get_where_clause_from_hive_part_list(self, hive_part_list: list):
        """__get_where_clause_from_hive_part_list.
        Generate a where clause to filter a dataframe.

        :param hive_part_list: List of partitions keys to be used in the where clause
        :type hive_part_list: list

        :return: return the whare clause
        :rtype: str
        """
        first_or = True
        where_clause = ''
        int_partitions = self.__get_input_read_int_partitions()
        partition_keys = []
        for part in hive_part_list:
            where_clause_and = ''
            part_key_value_vet = part.split("/")
            first_and = True
            for part_key_value in part_key_value_vet:
                part_key = part_key_value.split("=")
                if part_key[0] not in partition_keys:
                    partition_keys.append(part_key[0])
                if part_key[0] in int_partitions:
                    value = part_key[1]
                else:
                    value = "'{}'".format(part_key[1])
                if first_and:
                    where_clause_and = "{}={}".format(part_key[0], value)
                    first_and = False
                else:
                    where_clause_and = "{} AND {}={}".format(where_clause_and, part_key[0], value)
            if first_or:
                where_clause = '({})'.format(where_clause_and)
                first_or = False
            else:
                where_clause = '{} OR ({})'.format(where_clause, where_clause_and)
            self.__partition_keys = partition_keys
        return where_clause

    @staticmethod
    def __get_hash_sha256(text: str):
        """__get_hash_sha256.
        Get a hash sha256 based on text

        :param text: Text to be hashed
        :type text: str

        :return: Hash value
        """
        return hashlib.sha256(text.encode('utf-8')).hexdigest()

    @staticmethod
    def __get_hash_sha512(text: str):
        """__get_hash_sha512.
        Get a hash sha512 based on text

        :param text: Text to be hashed
        :type text: str

        :return: Hash value
        """
        return hashlib.sha512(text.encode('utf-8')).hexdigest()

    @staticmethod
    def __get_aes(input: str, key: str, inmode: str, salt: str):
        """__get_aes.
        Encrypts a string using AES algorithm

        :param input:  String to be encrypted
        :type layout: str
        :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
        :type layout: str
        :param inmode:  AES modes, Example: aes_ctr, aes_ctru, aes_ofb, aes_cfb, aes_cbc, aes_gcm
        :type layout: str
        :param salt:  String for salting
        :type layout: str
        """
        if input is None or inmode is None:
            _logger.error("Input is None for AES encrypt")
            return None

        if key is None or len(key) not in (16, 24, 32):
            _logger.error("Error in AES KEY")
            return None

        try:
            iterationCount = 1036
            inkey = hashlib.pbkdf2_hmac('sha256', key.encode('utf-8'), salt.encode('utf-8'), iterationCount)

            mode = inmode.lower()
            msg = input.encode('utf-8')
            if mode == "aes_cbc":
                aesCipher = AES.new(inkey, AES.MODE_CBC)
                ct_bytes = aesCipher.encrypt(pad(msg, AES.block_size))
            elif mode == "aes_ctr":
                iv = random.randrange(256)
                aesCipher = AES.new(inkey, AES.MODE_CTR, initial_value=iv)
                ct_bytes = aesCipher.encrypt(msg)
            elif mode == "aes_ctru":
                aesCipher = AES.new(inkey, AES.MODE_CTR, nonce=b'')
                ct_bytes = aesCipher.encrypt(msg)
            elif mode == "aes_cfb":
                aesCipher = AES.new(inkey, AES.MODE_CFB)
                ct_bytes = aesCipher.encrypt(msg)
            elif mode == "aes_ofb":
                aesCipher = AES.new(inkey, AES.MODE_OFB)
                ct_bytes = aesCipher.encrypt(msg)
            elif mode == "aes_gcm":
                aesCipher = AES.new(inkey, AES.MODE_GCM)

            if mode == "aes_ctr":
                nonce = b64encode(aesCipher.nonce)
                ciphertext = b64encode(ct_bytes)
                nonce_mod = binascii.hexlify(nonce)
                ciphertext_mod = binascii.hexlify(ciphertext)
                return str(ciphertext_mod.decode('utf-8') + '|' + str(iv) + '|' + nonce_mod.decode('utf-8'))
            elif mode == "aes_ctru":
                ciphertext = b64encode(ct_bytes)
                ciphertext_mod = binascii.hexlify(ciphertext)
                return (ciphertext_mod.decode('utf-8'))
            elif mode in ("aes_ofb", "aes_cfb", "aes_cbc"):
                iv = b64encode(aesCipher.iv)
                ciphertext = b64encode(ct_bytes)
                iv_mod = binascii.hexlify(iv)
                ciphertext_mod = binascii.hexlify(ciphertext)
                return str(ciphertext_mod.decode('utf-8') + '|' + iv_mod.decode('utf-8'))
            elif mode == "aes_gcm":
                ciphertext, authTag = aesCipher.encrypt_and_digest(msg)
                nonce_mod = binascii.hexlify(aesCipher.nonce)
                ciphertext_mod = binascii.hexlify(ciphertext)
                authTag_mod = binascii.hexlify(authTag)
                return str(ciphertext_mod.decode('utf-8') + '|' + nonce_mod.decode('utf-8') + '|' + authTag_mod.decode(
                    'utf-8'))
        except Exception as ne:
            _logger.error("Exception AES Encrypt: {}".format(ne))

    @staticmethod
    def __get_daes(input: str, key: str, inmode: str, salt: str):
        """__get_daes.
        Decrypts a string using AES algorithm

        :param input:  String to be encrypted
        :type layout: str
        :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
        :type layout: str
        :param inmode:  AES modes, Example: aes_ctr, aes_ctru, aes_ofb, aes_cfb, aes_cbc, aes_gcm
        :type layout: str
        :param salt:  String for salting
        :type layout: str
        """
        if input is None or inmode is None:
            _logger.error("Input is None for AES decrypt")
            return None

        if key is None or len(key) not in (16, 24, 32):
            _logger.error("Error in AES KEY")
            return None

        try:
            mode = inmode.lower()
            iterationCount = 1036
            inkey = hashlib.pbkdf2_hmac('sha256', key.encode('utf-8'), salt.encode('utf-8'), iterationCount)

            if mode == "aes_ctr_decrypt":
                (ciphertext_p, iv_p, nonce_p) = input.split('|')
                ciphertext_ptmp = binascii.unhexlify(ciphertext_p.encode('utf-8')).decode('utf-8')
                nonce_ptmp = binascii.unhexlify(nonce_p.encode('utf-8')).decode('utf-8')
                iv = iv_p
                ciphertext = b64decode(ciphertext_ptmp)
                nonce = b64decode(nonce_ptmp)
            elif mode == "aes_ctru_decrypt":
                (ciphertext_p) = input
                ciphertext_ptmp = binascii.unhexlify(ciphertext_p.encode('utf-8')).decode('utf-8')
                ciphertext = b64decode(ciphertext_ptmp)
            elif mode in ("aes_ofb_decrypt", "aes_cfb_decrypt", "aes_cbc_decrypt"):
                (ciphertext_p, iv_p) = input.split('|')
                iv_ptmp = binascii.unhexlify(iv_p.encode('utf-8')).decode('utf-8')
                ciphertext_ptmp = binascii.unhexlify(ciphertext_p.encode('utf-8')).decode('utf-8')
                iv = b64decode(iv_ptmp)
                ciphertext = b64decode(ciphertext_ptmp)
            elif mode == "aes_gcm_decrypt":
                (ciphertext_p, nonce_p, authTag_p) = input.split('|')
                nonce_ptmp = binascii.unhexlify(nonce_p.encode('utf-8'))
                ciphertext_ptmp = binascii.unhexlify(ciphertext_p.encode('utf-8'))
                authTag_ptmp = binascii.unhexlify(authTag_p.encode('utf-8'))

            if mode == "aes_cbc_decrypt":
                aesCipher = AES.new(inkey, AES.MODE_CBC, iv)
                plaintext = unpad(aesCipher.decrypt(ciphertext), AES.block_size)
                return str(plaintext.decode('utf-8'))
            elif mode == "aes_ctr_decrypt":
                aesCipher = AES.new(inkey, AES.MODE_CTR, initial_value=int(iv), nonce=nonce)
                plaintext = aesCipher.decrypt(ciphertext)
                return str(plaintext.decode('utf-8'))
            elif mode == "aes_ctru_decrypt":
                aesCipher = AES.new(inkey, AES.MODE_CTR, nonce=b'')
                plaintext = aesCipher.decrypt(ciphertext)
                return str(plaintext.decode('utf-8'))
            elif mode == "aes_cfb_decrypt":
                aesCipher = AES.new(inkey, AES.MODE_CFB, iv)
                plaintext = aesCipher.decrypt(ciphertext)
                return str(plaintext.decode('utf-8'))
            elif mode == "aes_ofb_decrypt":
                aesCipher = AES.new(inkey, AES.MODE_OFB, iv)
                plaintext = aesCipher.decrypt(ciphertext)
                return str(plaintext.decode('utf-8'))
            elif mode == "aes_gcm_decrypt":
                aesCipher = AES.new(inkey, AES.MODE_GCM, nonce_ptmp)
                plaintext = aesCipher.decrypt_and_verify(ciphertext_ptmp, authTag_ptmp)
                return str(plaintext.decode('utf-8'))

        except Exception as ne:
            _logger.error("Exception AES Decrypt: {}".format(ne))

    @staticmethod
    def __get_fte_encrypt(input: str, key: str, masktype: str):
        """__get_fte_encrypt.
               Encrypts a string using FTE algorithm

               :param input:  String to be encrypted
               :type layout: str
               :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
               :type layout: str
               :param masktype: Mask type to get the digits, Example: fte_enc, fte_enc_1, fte_enc_12
               :type layout: str
               """
        if input is None:
            return None

        if key is None or len(key) not in (16, 24, 32):
            _logger.error("Error in FTE KEY")
            return None

        try:

            printable = ' 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'
            input_v = input
            key_v = key

            def get_fpe(input_v, key_v):
                if input_v.isdigit():
                    text_enc = pyffx.Integer(key_v.encode('utf-8'), length=len(input_v))
                else:
                    text_enc = pyffx.String(key_v.encode('utf-8'), alphabet=printable, length=len(input_v))
                encrypt_v = str(text_enc.encrypt(input_v))
                if input_v.isdigit() and len(encrypt_v) < len(input_v):
                    encrypt_v = str(encrypt_v).rjust(len(input_v), '0')
                return encrypt_v

            fte_val = masktype.split("_")
            if fte_val != None and len(fte_val) >= 3:
                encryptn = int(fte_val[2] or 0)
                if encryptn != 0 and int(encryptn) < len(input):
                    digcypher = get_fpe(input[0:encryptn], key)
                    rdigcypher = input[encryptn:len(input)]
                    encrypt = digcypher + rdigcypher
                else:
                    encrypt = get_fpe(input, key)
            else:
                encrypt = get_fpe(input, key)
            return encrypt
        except Exception as ne:
            _logger.error("Exception FTE Encrypt: {}".format(ne))

    @staticmethod
    def __get_fte_dcrypt(input: str, key: str, masktype: str):
        """__get_fte_dcrypt.
        Decrypts a string using FTE algorithm

        :param input:  String to be decrypted
        :type layout: str
        :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
        :type layout: str
        :param masktype: Mask type to get the digits, Example: fte_enc, fte_enc_1, fte_enc_12
        :type layout: str
        """
        if input is None:
            return None

        if key is None or len(key) not in (16, 24, 32):
            _logger.error("Error in FTE KEY")
            return None

        try:
            printable = ' 0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'

            def get_dfpe(input_v, key_v):
                if input_v.isdigit():
                    text_enc = pyffx.Integer(key_v.encode('utf-8'), length=len(input_v))
                else:
                    text_enc = pyffx.String(key_v.encode('utf-8'), alphabet=printable, length=len(input_v))
                decrypt_v = str(text_enc.decrypt(input_v))
                if input_v.isdigit() and len(decrypt_v) < len(input_v):
                    decrypt_v = str(decrypt_v).rjust(len(input_v), '0')
                return decrypt_v
            
            fte_val = masktype.split("_")
            if fte_val != None and len(fte_val) >= 3:
                encryptn = int(fte_val[2] or 0)
                if encryptn != 0 and int(encryptn) < len(input):
                    digcypher = get_dfpe(input[0:encryptn], key)
                    rdigcypher = input[encryptn:len(input)]
                    decrypt = digcypher + rdigcypher
                else:
                    decrypt = get_dfpe(input, key)
            else:
                decrypt = get_dfpe(input, key)
            return decrypt
        except Exception as ne:
            _logger.error("Exception FTE Decrypt: {}".format(ne))

    @staticmethod
    def __get_rsa_encrypt(input: str, key: str):

        """__get_rsa_encrypt.
            :param input:  String to be encrypted
            :type layout: str
            :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
            :type layout: str
        """
        if input is None:
            return None

        if key is None:
            _logger.error("Error: key is empty")
            return None

        try:
            publicKey = RSA.importKey(key)
            cipher = PKCS1_OAEP.new(publicKey)
            return b64encode(cipher.encrypt(input.encode())).decode()
        except Exception as ne:
            _logger.error("Exception Encrypt RSA: {}".format(ne))

    @staticmethod
    def __get_rsa_dcrypt(input: str, key: str):
        """__get_rsa_dcrypt.
        Decrypts a string using RSA algorithm

        :param input:  String to be decrypted
        :type layout: str
        :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
        :type layout: str
        """
        if input is None:
            return None

        if key is None:
            _logger.error("Error: key is empty")
            return None

        try:
            privateKey = RSA.importKey(key)
            cipher = PKCS1_OAEP.new(privateKey)
            return cipher.decrypt(b64decode(input)).decode()
        except Exception as ne:
            _logger.error("Exception Decrypt RSA: {}".format(ne))

    @staticmethod
    def __get_bwf(input: str, key: str):
        """__get_bwf.
        Encrypts a string using Blowfish algorithm

        :param input:  String to be encrypted
        :type layout: str
        :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
        :type layout: str
        """
        if input is None:
            _logger.error("Input is None for BF encrypt")
            return None

        if key is None or len(key) not in (16, 24, 32):
            _logger.error("Error in BF KEY")
            return None

        try:
            msg = input.encode('utf-8')
            bs = Blowfish.block_size
            blfCipher = Blowfish.new(key.encode(), Blowfish.MODE_CBC)
            plen = bs - len(msg) % bs
            padding = [plen] * plen
            padding = pack('b' * plen, *padding)
            iv = b64encode(blfCipher.iv)
            ciphertext = b64encode(blfCipher.encrypt(msg + padding))
            iv_mod = binascii.hexlify(iv)
            ciphertext_mod = binascii.hexlify(ciphertext)
            return str(ciphertext_mod.decode('utf-8') + '|' + iv_mod.decode('utf-8'))
        except Exception as ne:
            _logger.error("Exception Blowfish Encrypt: {}".format(ne))

    @staticmethod
    def __get_dbwf(input: str, key: str):
        """__get_dbwf.
        Decrypts a string using BlowFlish algorithm

        :param input:  Encrypted message
        :type layout: str
        :param key: Key to be used for the algorithm, Example: abcdefghijklmnopqrstuvwxascertgb
        :type layout: str
        """
        if input is None:
            _logger.error("Input is None for BF encrypt")
            return None

        if key is None or len(key) not in (16, 24, 32):
            _logger.error("Error in BF KEY")
            return None

        try:
            (ciphertext_p, iv_p) = input.split('|')
            iv_ptmp = binascii.unhexlify(iv_p.encode('utf-8')).decode('utf-8')
            ciphertext_ptmp = binascii.unhexlify(ciphertext_p.encode('utf-8')).decode('utf-8')
            iv = b64decode(iv_ptmp)
            ciphertext = b64decode(ciphertext_ptmp)
            blfCipher = Blowfish.new(key.encode(), Blowfish.MODE_CBC, iv)
            plaintext = blfCipher.decrypt(ciphertext)
            return str(plaintext.decode('utf-8'))
        except Exception as ne:
            _logger.error("Exception Blowfish Decrypt: {}".format(ne))

    @staticmethod
    def __get_hash_sha512_256(text: str):
        """__get_hash_sha512_256.
        Get a hash sha256 and sha512 based on text

        :param text:
        :type text: str

        :return: Hash value
        """
        return DatamaskConfProcess.__get_hash_sha256(DatamaskConfProcess.__get_hash_sha512(text))

    @staticmethod
    def __mask(text, salt, layout: str, mask_type: str, format_re: str, key=str):
        """__mask.
        Mask a text entries based on parameters

        :param text:  Text to be masked
        :param salt: Salt value to be used in before mask
        :param layout: Layout to be used to replace the salt value and the field value. $VALUE will be replaced to the field value and $SALT will be replaced to the salt value
        :type layout: str
        :param mask_type: Type of algorithms Sha256, sha512, others
        :type mask_type: str
        :param format_re: Regular expression to be aplied in the field value before mask.
        :type format_re: str
        :param key: Key value for encrypt and decrypt algorithms
        """

        if isinstance(text, bytes):
            text = str(text.encode('utf-8'))
        else:
            text = str(text)

        if isinstance(salt, bytes):
            salt = str(salt.encode('utf-8'))
        else:
            salt = str(salt)

        if text == "" or text == "NI" or text == "null":
            return text
        text_ori = text
        text = re.search(format_re, text).group()
        text_tmp = layout.replace("$SALT", salt)
        text = text_tmp.replace("$VALUE", text)
        if mask_type == "sha256":
            return DatamaskConfProcess.__get_hash_sha256(text)
        if mask_type == "sha512":
            return DatamaskConfProcess.__get_hash_sha512(text)
        if mask_type == "sha512/sha256":
            return DatamaskConfProcess.__get_hash_sha512_256(text)
        if mask_type in ("aes_ctr", "aes_ctru", "aes_ofb", "aes_cfb", "aes_cbc", "aes_gcm"):
            return DatamaskConfProcess.__get_aes(text_ori, key, mask_type, salt)
        if mask_type == "fte_enc":
            return DatamaskConfProcess.__get_fte_encrypt(text_ori, key, mask_type)
        if mask_type == "rsa_encrypt":
            return DatamaskConfProcess.__get_rsa_encrypt(text_ori, key)
        if mask_type == "blowfish":
            return DatamaskConfProcess.__get_bwf(text_ori, key)

    @staticmethod
    def __unmask(text, salt, layout: str, mask_type: str, format_re: str, key=str):
        """__unmask.
        Unmask a text entries based on parameters

        :param text:  Text to be masked
        :param salt: Salt value to be used in before mask
        :param layout: Layout to be used to replace the salt value and the field value. $VALUE will be replaced to the field value and $SALT will be replaced to the salt value
        :type layout: str
        :param mask_type: Type of algorithms Sha256, sha512, others
        :type mask_type: str
        :param format_re: Regular expression to be aplied in the field value before mask.
        :type format_re: str
        :param key: Key value for encrypt and decrypt algorithms
        """
        if isinstance(text, bytes):
            text = str(text.encode('utf-8'))
        else:
            text = str(text)

        if isinstance(salt, bytes):
            salt = str(salt.encode('utf-8'))
        else:
            salt = str(salt)

        if text == "" or text == "NI" or text == "null":
            return text

        text_ori = text

        if mask_type in ("aes_ctr_decrypt", "aes_ctru_decrypt", "aes_ofb_decrypt", "aes_cfb_decrypt", "aes_cbc_decrypt", "aes_gcm_decrypt"):
            return DatamaskConfProcess.__get_daes(text_ori, key, mask_type, salt)
        if mask_type == "fte_enc_decrypt":
            mktp = mask_type.replace("decrypt","")
            return DatamaskConfProcess.__get_fte_dcrypt(text_ori, key, mktp)
        if mask_type == "rsa_decrypt":
            return DatamaskConfProcess.__get_rsa_dcrypt(text_ori, key)
        if mask_type == "blowfish_decrypt":
            return DatamaskConfProcess.__get_dbwf(text_ori, key)

    def __get_mask_fiels(self):
        """__get_mask_fiels.
        :return: Mask field iter

        """
        return iter(self.__mask_fields)

    def __get_domain(self, domainName: str):
        """__get_domain.

        :param domainName: Domain name to be searched
        :type domainName: str

        :return: The domain or None if it could not find
        """
        if domainName in self.__domains_idx:
            return self.__domains[self.__domains_idx[domainName]]
        else:
            return None

    def __get_salt(self, saltName: str):
        """__get_salt.

        :param saltName:
        :type saltName: str

        :return: The salt value or None if it could not find
        """
        if saltName in self.__salt_idx:
            return self.__salt[self.__salt_idx[saltName]]["SaltValue"]
        else:
            return None

    def __get_df_mask_spark(self, spark, df_input):
        """__get_df_mask_spark.
        Get the mask spark dataframe and update the all the reverse dataset if it was eanble.

        :param spark: Spark session
        :param df_input: The input spark dataframe

        :return: Spark dataframe with the mask field
        """
        from pyspark.sql.functions import udf, lit, col
        from pyspark.sql.utils import AnalysisException
        df_mask = df_input
        sqlMask = udf(DatamaskConfProcess.__mask)
        sqlUMask = udf(DatamaskConfProcess.__unmask)
        for item in self.__get_mask_fiels():
            if "Active" not in item:
                item["Active"] = True
               
            if "FormatRE" not in item:
                item["FormatRE"] = ".*"
                
                
            if item["Active"] == True:
                domain_config = self.__get_domain(item["DomainName"])
                if not domain_config:
                    raise DatamaskConfProcessError("DomainName [#{}#] was not found in Domains [#{}#]".format(item["DomainName"],self.__domains))
                salt = self.__get_salt(domain_config["SaltName"])
                if not salt:
                    raise DatamaskConfProcessError("SaltName {} does not exist in salt file".format(item["SaltName"]))

                if domain_config["MaskType"] in (
                "aes_ctr", "aes_ctru", "aes_ofb", "aes_cfb", "aes_cbc", "aes_gcm", "blowfish", "fte_enc"):
                    # Insert here the kms option.
                    # Check if kms ARN exists, and if it does, use it as the key for encryption
                    if "EncryptKey" in domain_config and len(domain_config["EncryptKey"]) > 0:
                        encrypt_key = domain_config["EncryptKey"]
                        df_mask = df_mask.withColumn(
                            item["MaskFieldName"],
                            sqlMask(item["FieldName"],
                                    lit(salt),
                                    lit(domain_config["Layout"]),
                                    lit(domain_config["MaskType"]),
                                    lit(item["FormatRE"]), lit(domain_config["EncryptKey"])
                                    )).cache()
                    else:
                        raise DatamaskConfProcessError(
                            "EncryptKey does not exist in config file for mask type domains ")

                elif domain_config["MaskType"] == 'rsa_encrypt':
                    if "RSAPublicKey" in domain_config and len(domain_config["RSAPublicKey"]) > 0:
                        public_key_path = domain_config["RSAPublicKey"]
                        public_key = None
                        try:
                            if "s3://" in public_key_path:
                                bucket = public_key_path.split('/')[2]
                                key = '/'.join(public_key_path.split('/')[3:]) 
                                s3 = boto3.resource('s3')
                                obj = s3.Object(bucket, key)
                                public_key = obj.get()['Body'].read().decode('utf-8')
                            else:
                                f_public = open(public_key_path, 'r')
                                public_key = f_public.read()
                                f_public.close()

                        except IOError:
                            DatamaskConfProcessError("%s file is not exists or not accessible" % public_key_path)
                            sys.exit()

                        df_mask = df_mask.withColumn(
                            item["MaskFieldName"],
                            sqlMask(item["FieldName"],
                                    lit(salt),
                                    lit(domain_config["Layout"]),
                                    lit(domain_config["MaskType"]),
                                    lit(item["FormatRE"]), lit(public_key)
                                    )).cache()
                    else:
                        raise DatamaskConfProcessError(
                            "Public key path does not exist in config file for mask type domains ")

                elif domain_config['MaskType'] == 'rsa_decrypt':
                    if "RSAEncryptedPrivateKey" in domain_config and len(domain_config["RSAEncryptedPrivateKey"]) > 0 \
                            and "RSACMKeyId" in domain_config and len(domain_config["RSACMKeyId"]) > 0 \
                            and len(domain_config["AWSRegion"]) > 0:

                        time.clock = time.process_time
                        encrypted_private_key_path = domain_config["RSAEncryptedPrivateKey"]
                        CMKeyId = domain_config["RSACMKeyId"]
                        encrypted_public_key = None
                        try:
                            if "s3://" in encrypted_private_key_path:
                                bucket = encrypted_private_key_path.split('/')[2]
                                key = '/'.join(encrypted_private_key_path.split('/')[3:]) 
                                s3 = boto3.resource('s3')
                                obj = s3.Object(bucket, key)
                                encrypted_public_key = obj.get()['Body'].read()
                            else:
                                f_encrypted_public = open(encrypted_private_key_path, 'rb')
                                encrypted_public_key = f_encrypted_public.read()
                                f_encrypted_public.close()

                        except IOError:
                            DatamaskConfProcessError("%s file is not exists or not accessible" % public_key_path)
                            sys.exit()

                        client = boto3.client('kms', domain_config["AWSRegion"])

                        kms_respond = client.decrypt(
                            CiphertextBlob=encrypted_public_key,
                            KeyId=CMKeyId,
                            EncryptionAlgorithm='SYMMETRIC_DEFAULT'
                        )

                        df_mask = df_mask.withColumn(
                            item["MaskFieldName"],
                            sqlUMask(item["FieldName"],
                                    lit(salt),
                                    lit(domain_config["Layout"]),
                                    lit(domain_config["MaskType"]),
                                    lit(item["FormatRE"]), lit(kms_respond['Plaintext'])
                                    )).cache()

                elif domain_config["MaskType"] in (
                "aes_ctr_decrypt", "aes_ctru_decrypt", "aes_ofb_decrypt", "aes_cfb_decrypt", "aes_cbc_decrypt", "aes_gcm_decrypt", "blowfish_decrypt", "fte_enc_decrypt"):
                    # Insert here the kms option.
                    # Check if kms ARN exists, and if it does, use it as the key for encryption
                    if "EncryptKey" in domain_config and len(domain_config["EncryptKey"]) > 0:
                        encrypt_key = domain_config["EncryptKey"]
                        df_mask = df_mask.withColumn(
                            item["MaskFieldName"],
                            sqlUMask(item["FieldName"],
                                    lit(salt),
                                    lit(domain_config["Layout"]),
                                    lit(domain_config["MaskType"]),
                                    lit(item["FormatRE"]), lit(domain_config["EncryptKey"])
                                    )).cache()
                    else:
                        raise DatamaskConfProcessError(
                            "EncryptKey does not exist in config file for mask type domains ")

                else:
                    df_mask = df_mask.withColumn(
                        item["MaskFieldName"],
                        sqlMask(item["FieldName"],
                                lit(salt),
                                lit(domain_config["Layout"]),
                                lit(domain_config["MaskType"]),
                                lit(item["FormatRE"])
                                ))

                if "ReverseDataset" in domain_config and domain_config["ReverseDataset"]["Active"]:
                    if self.__reverse_dataset_path:
                        domain_config["ReverseDataset"]["DatasetPath"] = "{}/{}".format(self.__reverse_dataset_path, item["DomainName"])
                        
                    df_mask_reverse = df_mask.select(item["FieldName"], item["MaskFieldName"])

                    df_mask_reverse = df_mask_reverse. \
                        withColumnRenamed(item["FieldName"], domain_config["FieldAlias"]). \
                        withColumnRenamed(item["MaskFieldName"], domain_config["MaskFieldAlias"])

                    job_name_partition = False
                    if 'JobNamePartition' in domain_config['ReverseDataset'] and domain_config['ReverseDataset'][
                        'JobNamePartition']:
                        job_name_partition = True

                    if 'JobNamePartitionFieldName' in domain_config['ReverseDataset']:
                        job_name_partition_field_name = domain_config['ReverseDataset']['JobNamePartitionFieldName']
                    else:
                        job_name_partition_field_name = 'JobName'

                    if 'BackupDatasetPath' in domain_config['ReverseDataset']:
                        backup_path = domain_config['ReverseDataset']['BackupDatasetPath']
                    else:
                        reverse_updir = "/".join(domain_config['ReverseDataset']['DatasetPath'].split("/")[:-1])
                        reverse_dir = domain_config['ReverseDataset']['DatasetPath'].split("/")[-1]
                        backup_path = '{}/{}_bkp'.format(reverse_updir, reverse_dir)

                    year_label = 'year'
                    month_label = 'month'
                    day_label = 'day'
                    hour_label = 'hour'
                    id_label = 'id'
                    job_label = 'job'

                    if 'BackupDatasetPartitionLabels' in domain_config['ReverseDataset']:
                        if 'Year' in domain_config['ReverseDataset']['BackupDatasetPartitionLabels']:
                            year_label = domain_config['ReverseDataset']['BackupDatasetPartitionLabels']['Year']
                        if 'Month' in domain_config['ReverseDataset']['BackupDatasetPartitionLabels']:
                            month_label = domain_config['ReverseDataset']['BackupDatasetPartitionLabels']['Month']
                        if 'Day' in domain_config['ReverseDataset']['BackupDatasetPartitionLabels']:
                            day_label = domain_config['ReverseDataset']['BackupDatasetPartitionLabels']['Day']
                        if 'Hour' in domain_config['ReverseDataset']['BackupDatasetPartitionLabels']:
                            hour_label = domain_config['ReverseDataset']['BackupDatasetPartitionLabels']['Hour']
                        if 'Id' in domain_config['ReverseDataset']['BackupDatasetPartitionLabels']:
                            id_label = domain_config['ReverseDataset']['BackupDatasetPartitionLabels']['Id']

                    year = datetime.datetime.now().year
                    month = datetime.datetime.now().month
                    day = datetime.datetime.now().day
                    hour = datetime.datetime.now().hour
                    id_uuid = str(uuid.uuid4())
                    job = self.__job
                    #job = "testeDataMask"

                    partition_backup = '{}={}/{}={}/{}={}/{}={}/{}={}/{}={}'.format( \
                        year_label, year, \
                        month_label, month, \
                        day_label, day, \
                        hour_label, hour, \
                        job_label, job, \
                        id_label, id_uuid)

                    backup_full_path = '{}/{}'.format(backup_path, partition_backup)

                    # df_all_dedup_reverse = df_reverse_dataset.where(df_reverse_dataset.MaskTableReference == table_path).union(df_mask_reverse).dropDuplicates()

                    df_mask_reverse.dropDuplicates()

                    if 'BackupCoalesce' in domain_config['ReverseDataset']:
                        df_mask_reverse = df_mask_reverse.coalesce(domain_config['ReverseDataset']['BackupCoalesce'])

                    df_mask_reverse \
                        .write \
                        .format("parquet") \
                        .option("compression", "snappy") \
                        .option("path", backup_full_path) \
                        .mode("append") \
                        .save()

                    if not 'KeepOnlyBackp' in domain_config['ReverseDataset'] or not domain_config['ReverseDataset'][
                        'KeepOnlyBackp']:
                        df_backup = spark.read.parquet(backup_full_path)

                        if job_name_partition:
                            df_backup = df_backup.select(domain_config["FieldAlias"],
                                                         domain_config["MaskFieldAlias"]).withColumn(
                                job_name_partition_field_name, lit(job))
                            df_backup.schema[job_name_partition_field_name].nullable = True
                            df_mask_reverse = df_mask_reverse.withColumn(job_name_partition_field_name, lit(job))
                            df_mask_reverse.schema[job_name_partition_field_name].nullable = True
                        else:
                            df_backup = df_backup.select(domain_config["FieldAlias"], domain_config["MaskFieldAlias"])

                        schema = df_mask_reverse.schema
                        path_exists = True

                        try:
                            df_reverse_dataset = spark.read.parquet(domain_config['ReverseDataset']['DatasetPath'])
                        except AnalysisException as e:
                            _logger.warning(
                                "Infering that is the first load in this table partition log path, directory does not exists")
                            df_reverse_dataset = spark.createDataFrame([], schema)
                            path_exists = False

                        _logger.info("df_mask_reerse.schema:## {}  ##".format(df_mask_reverse.schema))
                        _logger.info("df_reverse_dataset.schema:## {}  ##".format(df_reverse_dataset.schema))

                        if path_exists and df_mask_reverse.schema != df_reverse_dataset.schema:
                            raise DatamaskConfProcessError(
                                "The new Reverse Dataset has diferent schema than the Reverse Dataset in storage, please delete or migrate the currently Reverse Dataset in storage to the new version of Datamask Framework")

                        _logger.info("df_mask_reverse.schema:## {}##".format(df_mask_reverse.schema))
                        _logger.info("df_reverse_dataset.schema:## {}##".format(df_reverse_dataset.schema))

                        if job_name_partition:
                            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
                            df_reverse_dataset = df_reverse_dataset.filter(
                                col(job_name_partition_field_name) == lit(job))
                        else:
                            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

                        df_union = df_backup.union(df_mask_reverse).dropDuplicates()

                        write_stm = df_union \
                            .coalesce(domain_config['ReverseDataset']['Coalesce']) \
                            .write \
                            .format("parquet") \
                            .option("compression", "snappy") \
                            .option("path", domain_config['ReverseDataset']['DatasetPath']) \
                            .mode("overwrite") \
 
                        if job_name_partition:
                            write_stm.partitionBy(job_name_partition_field_name)

                        if "ReverseTable" in domain_config['ReverseDataset'] \
                                and "ReverseDatabase" in domain_config['ReverseDataset']:
                            write_stm.saveAsTable('{}.{}'.format(domain_config['ReverseDataset']["ReverseDatabase"],
                                                                 domain_config['ReverseDataset']["ReverseTable"]))
                        else:
                            write_stm.save()
                else:
                    _logger.warning("ReverseDataset is not enable")
                df_mask = df_mask.drop(item["FieldName"])
                if not ("KeepMaskName" in item and item["KeepMaskName"]):
                    df_mask = df_mask.withColumnRenamed(item["MaskFieldName"], item["FieldName"])
        return df_mask

    def __filter_partitions_spark(self, df_input, part_list: list):
        """__filter_partitions_spark.
        Filter partitions in a spark dataframe

        :param df_input: Spark dataframe input
        :param part_list: Partitions key list
        :type part_list: list

        :return: Filtered spark dataframe

        """
        where_clause = self.__get_where_clause_from_hive_part_list(part_list)
        return df_input.where(where_clause)

    def process_spark(self, df_input, spark):
        """process_spark.
        Process the data mask pipeline with spark

        :param part_list: Partition Key list to be filtered
        :type part_list: list
        :param spark: Spark Session
        """
        #df_filtered = df_input
        #if len(part_list) > 0:
        #    df_filtered = self.__filter_partitions_spark(df_filtered, part_list)

        df_mask = self.__get_df_mask_spark(spark, df_input)
        return df_mask

    def process_spark_streaming(self, topic_list: list):
        """process_spark_streaming.
        Start the streaming processing to mask a stream

        :param topic_list:
        :type topic_list: list
        """
        pass
        return


def execute (spark, array_input, parameters,dynamic_parameters):
    _logger.info("*** Starting udf execution, [{}],[{}],[{}] ***".format(spark, array_input, parameters))
    
    if len(array_input) != 1:
        raise  DatamaskConfProcessError("DataMask UDF is compatible with only one dataframe")
    
    df_input = array_input["df_input"]

    datamask = DatamaskConfProcess(parameters, dynamic_parameters['JobName'])
    df_output = datamask.process_spark(df_input,spark)

    return df_output
