# encoding: utf-8
import argparse
import os
import shelve
import boto3
import json
from datetime import datetime
import random



class ConfigError(Exception):
    pass

def json_datetime_serial(obj):
    """JSON serializer for date objects not serializable by default json code"""

    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")


class glacier_shelve(object):
    """
    Context manager for shelve
    """
    def __init__(self, shelve_file):
        self.shelve_file = shelve_file

    def __enter__(self):
        self.shelve = shelve.open(self.shelve_file)

        if "jobs" not in self.shelve:
            self.shelve["jobs"] = dict()

        if "archive_objects" not in self.shelve:
            self.shelve["archive_objects"] = dict()

        return self.shelve

    def __exit__(self, exc_type, exc_value, traceback):
        self.shelve.close()


class GlacierVault:
    """
    Wrapper for uploading/download archive to/from Amazon Glacier Vault
    Makes use of shelve to store archive id corresponding to filename and waiting jobs.

    Backup:
    >>> GlacierVault("myvault")upload("myarchive", "myfile")

    Restore:
    >>> GlacierVault("myvault")retrieve("myarchive", "myfile")

    or to wait until the job is ready:
    >>> GlacierVault("myvault")retrieve("myarchive", "serverhealth2.py", True)
    """
    def __init__(self, vault_name, access_key=None, secret_key=None, shelve_file="~/.glaciervault.db"):
        """
        Initialize the vault
        """
        # layer2 = boto.connect_glacier(aws_access_key_id = ACCESS_KEY_ID,
        #                             aws_secret_access_key = SECRET_ACCESS_KEY)
        # Or via the Session
        if access_key and secret_key:
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        else:
            session = boto3.Session()

        glacier = session.resource('glacier')
        self.vault = glacier.Vault('-', vault_name)
        self.shelve_file = os.path.expanduser(shelve_file)
        # self.vault = layer2.get_vault(vault_name)

    def upload(self, fileobj, arch_descr, print_info=False, dummy=False):
        """
        Upload filename and store the archive id for future retrieval
        """

        if print_info:
            print("Uploading '{}'".format(arch_descr))

        fileobj.seek(0)
        if not dummy:
            archive = self.vault.upload_archive(
                archiveDescription=json.dumps(arch_descr, default=json_datetime_serial),
                body=fileobj
            )
        else:
            arch_id = random.randint(0, 9999999999)
            d = open("vault/f{}.txt".format(arch_id), 'wb')
            d.write(fileobj.read())
            d.close()

        # Storing the filename => archive_id data.
        with glacier_shelve(self.shelve_file) as d:
            archive_objects = d["archive_objects"]

            if not dummy:
                arch_descr['id'] = archive.id
            else:
                arch_descr['id'] = arch_id
            archive_objects[arch_descr['name']] = {arch_descr['id']: arch_descr}

            #write to shelve
            d["archive_objects"] = archive_objects

    def get_archive_list(self, arch_obj_name=None):
        """

        """
        with glacier_shelve(self.shelve_file) as d:
            archive_objects = d["archive_objects"]

            if arch_obj_name is None:
                return archive_objects

            if arch_obj_name in archive_objects:
                return archive_objects[arch_obj_name]

        return None

    # def get_latest_name(self, search_name):
    #     with glacier_shelve(self.shelve_file) as d:
    #         archives = d["archives"]
    #
    #         latest = None
    #         for archive_name in sorted(archives):
    #             m = re.search('(.*)_\d{4}-\d{2}-\d{2}', archive_name)
    #             rest = None
    #             if m:
    #                 rest = m.group(1)
    #             if archive_name == search_name or rest == search_name:
    #                 latest = archive_name
    #
    #     if latest is None:
    #         raise NameError('No latest archive found for \'{}\''.format(search_name))
    #
    #     return latest

    def retrieve(self, archive_id, fileobj, wait_mode=False, print_info=False, dummy=False):
        """
        Initiate a Job, check its status, and download the archive when it's completed.
        """
        if dummy:
            d = open("vault/f{}.txt".format(archive_id), 'rb')
            fileobj.write(d.read())
            d.close()
            return True
        # archive_id = self.get_archive_id(archive_name)
        # if not archive_id:
        #     raise NameError('No archive_id found for \'{}\''.format(archive_name))
        #     return
        # elif print_info:
        #         print('Getting archive: {}'.format(archive_name))

        with glacier_shelve(self.shelve_file) as d:
            jobs = d["jobs"]
            job = None

            if archive_id in jobs:
                if print_info:
                    print('Some job for this archive found in shelve. Trying to load it')
                # The job is already in shelve
                job_id = jobs[archive_id]
                try:
                    job = self.vault.Job(job_id)
                    job.load()
                    if print_info:
                        print('Retrive job loaded from AWS.')
                except Exception as e:
                    job = None
                    # todo catch ResourceNotFoundException
                    if print_info:
                        print('Error while trying to load Job.')
                        print(e)

            if not job:
                if print_info:
                    print('No job for this archive found. Creating new retrive job.')
                # Job initialization
                job = self.vault.Archive(archive_id).initiate_archive_retrieval()
                # job = self.vault.retrieve_archive(archive_id)
                jobs[archive_id] = job.id
                job_id = job.id

            # Commiting changes in shelve
            d["jobs"] = jobs

        # checking manually if job is completed every 600 secondes instead of using Amazon SNS
        if wait_mode:
            import time
            while not job.completed:
                job = self.vault.Job(job_id)
                if print_info:
                    print("Job {action}: {status_code} ({creation_date}/{completion_date})".format(
                        action=job.action,
                        status_code=job.status_code,
                        creation_date=job.creation_date,
                        completion_date=job.completion_date))
                time.sleep(600)

        else:
            if print_info:
                print("Job {action}: {status_code} ({creation_date}/{completion_date})".format(
                    action=job.action,
                    status_code=job.status_code,
                    creation_date=job.creation_date,
                    completion_date=job.completion_date))

        if job.completed:
            if print_info:
                print("Downloading...")
            response = job.get_output()

            fileobj.write(response['body'].read())

            return True

        else:
            if print_info:
                print("Job not ready yet.")
            return False
