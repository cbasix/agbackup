from Crypto.Cipher import AES
import argparse
import json
from agcrypt import AESCipher
from aglacier import GlacierVault, ConfigError
import os
from datetime import date
import tarfile
import tempfile

class BackupObjectNotFound(Exception):
    pass

class NotReadyYet(Exception):
    pass


class Agbackup(object):

    def __init__(self, config_path):
        #load config file
        with open(config_path) as data_file:
            self.config = json.load(data_file)

        # test manditory config fields
        # if not 'access_key' in self.config:
            # raise ConfigError("access_key not in config")
        # if not 'secret_key' in self.config:
            # raise ConfigError("secret_key not in config")
        if not 'vault' in self.config:
            raise ConfigError("vault not in config")
        if not 'shelve_file' in self.config:
            raise ConfigError("shelve_file not in config")
        if not 'backup_objects' in self.config:
            raise ConfigError("backup_objects not in config")

        #init crypt
        if 'encryption_key' in self.config:
            self.crypt = AESCipher(self.config['encryption_key'])

        # check backup objects
        for backup_object in self.config['backup_objects']:
            if 'name' not in backup_object:
                raise ConfigError("backup_object '{}' has no 'name' attribute in config".format(object))
            if 'path' not in backup_object:
                raise ConfigError("backup_object '{}' has no 'name' attribute in config".format(object))
            if 'is_folder' not in backup_object:
                raise ConfigError("backup_object '{}' has no 'is_folder' attribute in config".format(object))
            if 'encrypt' in backup_object and backup_object['encrypt'] and self.crypt is None:
                raise ConfigError(
                    "backup_object '{}' has encypt activated but no enkyrpton_key is given in config".format(
                        object))

        #init glacier
        access_key = None
        if 'access_key' in self.config:
            access_key = self.config['access_key']

        secret_key = None
        if 'secret_key' in self.config:
            secret_key = self.config['secret_key']

        shelve_file = None
        if 'shelve_file' in self.config:
            shelve_file = self.config['shelve_file']

        self.vault = GlacierVault(self.config['vault'],
                                  access_key,
                                  secret_key,
                                  shelve_file)

    def backup(self, object_name):
        backup_objects = self.config["backup_objects"]
        if object_name is None:
            for backup_object in backup_objects:
                self._upload(backup_object)
        else:
            found = False
            for backup_object in backup_objects:
                if backup_object['name'] == object_name:
                    found = True
                    self._upload(backup_object)
                    break
            if not found:
                raise BackupObjectNotFound("backup_object '{}' not found in config".format(object_name))

    def _upload(self, backup_object):
        arch_file_description = backup_object['name']
        if 'add_date' not in backup_object or backup_object['add_date']:
            arch_file_description += "_"+date.today().isoformat()

        if os.path.isfile(backup_object['path']) and not backup_object['is_folder']:
            # self.vault.upload(backup_object['path'], name, print_info=True)
            backup_file = open(backup_object['path'], 'rb')
            self._upload_e(backup_object, arch_file_description, backup_file)

        elif os.path.isdir(backup_object['path']) and backup_object['is_folder']:
            # tar+zip it in temp folder
            targz = tempfile.TemporaryFile()
            self._make_tarfile(output_file=targz, source_dir=backup_object['path'])

            self._upload_e(backup_object, arch_file_description, targz)

            targz.close()

        else:
            raise ConfigError("backup_object '{}' has an invalid 'path' attribute "
                              "or attribute 'folder' is wrong in config".format(backup_object))

    def _upload_e(self, backup_object, arch_file_description, backup_file):
        backup_file.seek(0)
        print("File before encryption: ")
        print(backup_file.read())

        if 'encrypt' in backup_object and backup_object['encrypt']:
            encr = tempfile.TemporaryFile()
            self.crypt.encrypt(backup_file, encr)

            encr.seek(0)
            print("File after encryption: ")
            print(encr.read())

            self.vault.upload(fileobj=encr, archive_name=arch_file_description, print_info=True)

            encr.close()
        else:
            self.vault.upload(fileobj=backup_file, archive_name=arch_file_description, print_info=True)



    def retrive(self, object_name, out_path, force, wait):

        # config object is needed to get type, enkryption etc.
        backup_objects = self.config["backup_objects"]
        selected_object = None
        for backup_object in backup_objects:
            if backup_object['name'] == object_name:
                selected_object = backup_object
                break
        if selected_object is None:
            raise BackupObjectNotFound("backup_object '{}' not found in config".format(object_name))

        archived = tempfile.TemporaryFile()
        if 'add_date' not in selected_object or selected_object['add_date']:
            object_name = self.vault.get_latest_name(object_name)

        if not self.vault.retrieve(object_name, fileobj=archived, wait_mode=wait, print_info=True):
            raise NotReadyYet("AWS Glacier job not ready yet, it takes 3 to 5 hours")

        decrypted = None
        if selected_object['encrypt']:
            decrypted = tempfile.TemporaryFile()
            self.crypt.decrypt(archived, decrypted)
        else:
            decrypted = archived

        if selected_object['is_folder']:
            #unpack file and write into dest_folder
            self._extract_tarfile(output_folder=out_path, source_file=decrypted)

        else:
            if not os.path.exists(out_path) or force:
                # write file to out_path
                with open(out_path, 'wb') as f:
                    f.write(decrypted.read())
            else:
                raise FileExistsError(out_path)
                #todo throw file/folder does already exist

    @staticmethod
    def _make_tarfile(output_file, source_dir):
        with tarfile.open(fileobj=output_file, mode="w:gz") as tar:
            tar.add(source_dir, arcname=os.path.basename(source_dir))

    @staticmethod
    def _extract_tarfile(output_folder, source_file):
        # TODO Inspect tar before extract
        # Warning  Never extract archives from untrusted sources without prior inspection.
        # It is possible that files are created outside of path, e.g. members that have absolute
        # filenames starting with "/" or filenames with two dots "..".
        source_file.seek(0)

        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        t = tarfile.open(fileobj=source_file, mode='r')
        t.extractall(output_folder)


def init_argparse():
    # create the top-level parser
    parser = argparse.ArgumentParser(prog='AmazonGlacierBackup')
    parser.add_argument('-c', dest='conf_file', help='Path to alternative config file', default=None)
    subparsers = parser.add_subparsers(help='There are 2 subcommands. archive and retrive', dest='mode')

    # create the parser for the "archive" command
    parser_a = subparsers.add_parser('archive', help='Archives the given object (must be in config allready) in amazon glacier')
    parser_a.add_argument('-name', help='Object name', default=None)


    # create the parser for the "retrive" command
    parser_b = subparsers.add_parser('retrive', help='Initiates the retrival of the object from amazon glacier')
    parser_b.add_argument('name', help='Object name')
    parser_b.add_argument('-o', dest='out_path', help='Save object to this file', default=None)
    parser_b.add_argument('-f', dest='force', help='Overwrites existing files', action='store_true')
    parser_b.add_argument('-w', action='store_true', dest='wait')

    return parser


def main():
    parser = init_argparse()
    arg = parser.parse_args()

    if arg.conf_file is None:
        agb = Agbackup('config.json')
    else:
        agb = Agbackup(arg.conf_file)

    if arg.mode == 'archive':
        agb.backup(arg.name)
    elif arg.mode == 'retrive' and arg.name is not None:
        agb.retrive(arg.name, arg.out_path, arg.force, arg.wait)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()