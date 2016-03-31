# encoding: utf-8
import argparse
import os
import shelve
import boto3
import json
import re



class ConfigError(Exception):
    pass


class glacier_shelve(object):
    """
    Context manager for shelve
    """
    def __init__(self, shelve_file):
        self.shelve_file = shelve_file

    def __enter__(self):
        self.shelve = shelve.open(self.shelve_file)

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

    def upload(self, filename=None,  archive_name=None, print_info=False, fileobj=None,):
        """
        Upload filename and store the archive id for future retrieval
        """
        if archive_name is None:
            archive_name = filename

        if print_info:
            print("Uploading file '{}' with name '{}'".format(filename, archive_name))
        # archive_id = self.vault.create_archive_from_file(
        # filename, description=filename)
        if fileobj is None:
            file = open(filename, "rb",)
        else:
            file = fileobj
            file.seek(0)
        archive = self.vault.upload_archive(
            archiveDescription=archive_name,
            body=file
        )
        # Storing the filename => archive_id data.
        with glacier_shelve(self.shelve_file) as d:
            if not "archives" in d:
                d["archives"] = dict()

            archives = d["archives"]
            archives[archive_name] = archive.id
            d["archives"] = archives

    def get_archive_id(self, archive_name):
        """
        Get the archive_id corresponding to the filename
        """
        with glacier_shelve(self.shelve_file) as d:
            if not "archives" in d:
                d["archives"] = dict()

            archives = d["archives"]

            if archive_name in archives:
                return archives[archive_name]

        return None

    def get_latest_name(self, search_name):
        with glacier_shelve(self.shelve_file) as d:
            if not "archives" in d:
                d["archives"] = dict()

            archives = d["archives"]

            latest = None
            for archive_name in sorted(archives):
                m = re.search('(.*)_\d{4}-\d{2}-\d{2}', archive_name)
                rest = None
                if m:
                    rest = m.group(1)
                if archive_name == search_name or rest == search_name:
                    latest = archive_name

        if latest is None:
            raise NameError('No latest archive found for \'{}\''.format(search_name))

        return latest

    def retrieve(self, archive_name, filename=None, wait_mode=False, print_info=False, fileobj=None):
        """
        Initiate a Job, check its status, and download the archive when it's completed.
        """
        if filename is None:
            filename = archive_name

        archive_id = self.get_archive_id(archive_name)
        if not archive_id:
            raise NameError('No archive_id found for \'{}\''.format(archive_name))
            return
        elif print_info:
                print('Getting archive: {}'.format(archive_name))

        with glacier_shelve(self.shelve_file) as d:
            if not "jobs" in d:
                d["jobs"] = dict()

            jobs = d["jobs"]
            job = None

            if archive_name in jobs:
                if print_info:
                    print('Some job for archive "{}" found in shelve. Trying to load id'.format(archive_name))
                # The job is already in shelve
                job_id = jobs[archive_name]
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
                jobs[archive_name] = job.id
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
            if fileobj is None:
                with open(filename, 'wb') as f:
                    f.write(response['body'].read())
                    if print_info:
                        print("Ready")
            else:
                fileobj.write(response['body'].read())
            return True

        else:
            if print_info:
                print("Job not ready yet.")
            return False

def init_argparse():
    # create the top-level parser
    parser = argparse.ArgumentParser(prog='AmazonGlacierBackupLowLevel')
    # parser.add_argument('--foo', action='store_true', help='foo help')
    subparsers = parser.add_subparsers(help='There are 2 subcommands. archive and retrive', dest='mode')

    # create the parser for the "archive" command
    parser_a = subparsers.add_parser('archive', help='Archives the given file in amazon glacier')
    parser_a.add_argument('archive_name', help='Archive name')
    parser_a.add_argument('filename', help='Filename')


    # create the parser for the "retrive" command
    parser_b = subparsers.add_parser('retrive', help='Initiates the retrival of the file from amazon glacier')
    parser_b.add_argument('archive_name', help='Archive name',)
    parser_b.add_argument('filename', help='Filename')
    parser_b.add_argument('-w', action='store_true', dest='wait')

    return parser


# sv_backup_job
# Access Key ID:
# AKIAJOOBFNOL2SBYPBGQ
# Secret Access Key:
# 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
ACCESS_KEY = "AKIAJOOBFNOL2SBYPBGQ"
SECRET_KEY = "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"
VAULT = 'sv_backup'

def main():
    # load config file
    with open('config.json') as data_file:
       config = json.load(data_file)

    # test manditory config fields
    if 'access_key' not in config:
        raise ConfigError("access_key not in config")
    if 'secret_key' not in config:
        raise ConfigError("secret_key not in config")
    if 'vault' not in config:
        raise ConfigError("vault not in config")
    if 'shelve_file' not in config:
        raise ConfigError("shelve_file not in config")

    parser = init_argparse()
    arg = parser.parse_args()

    # gv = GlacierVault(VAULT, ACCESS_KEY, SECRET_KEY)
    gv = GlacierVault(config['vault'],
                      config['access_key'],
                      config['secret_key'],
                      config['shelve_file'])

    if arg.mode == 'archive' and arg.filename is not None:
        # print('GlacierVault({vault}).upload({archive_name}, {filename} )'.format(vault=VAULT, filename=arg.filename, archive_name=arg.archive_name))
        gv.upload(filename=arg.filename, archive_name=arg.archive_name, print_info=True)
    elif arg.mode == 'retrive' and arg.filename is not None:
        # print('GlacierVault({vault}).retrive({archive_name}, {filename}, {wait})'.format(vault=VAULT, filename=arg.filename, wait=arg.wait, archive_name=arg.archive_name))
        gv.retrieve(filename=arg.filename, archive_name=arg.archive_name, wait_mode=arg.wait, print_info=True)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
    # s_main()