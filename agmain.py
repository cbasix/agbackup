from Crypto.Cipher import AES
import argparse
import json
from agcrypt import AESCipher
from aglacier import GlacierVault, ConfigError
import os
from datetime import datetime
import tarfile
import tempfile


class BackupObjectNotFound(Exception):
    pass


class NotReadyYet(Exception):
    pass


class Agbackup(object):
    def __init__(self, config_path):
        # load config file
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

        # init crypt
        if 'encryption_key' in self.config:
            self.crypt = AESCipher(self.config['encryption_key'])

        # check backup objects
        for backup_object in self.config['backup_objects']:
            if 'name' not in backup_object:
                raise ConfigError("backup_object '{}' has no 'name' attribute in config".format(object))
            if 'path' not in backup_object:
                raise ConfigError("backup_object '{}' has no 'path' attribute in config".format(object))
            if 'encrypt' in backup_object and backup_object['encrypt'] and self.crypt is None:
                raise ConfigError(
                    "backup_object '{}' has encypt activated but no enkyrpton_key is given in config".format(
                        object))

        # init glacier
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

        # when no object name given -> backup all
        if object_name is None:
            for backup_object in backup_objects:
                self.backup_element(backup_object)

        # else backup the given object
        else:
            found = False
            for backup_object in backup_objects:
                if backup_object['name'] == object_name:
                    found = True
                    self.backup_element(backup_object)
                    break
            if not found:
                raise BackupObjectNotFound("backup_object '{}' not found in config".format(object_name))

    def backup_element(self, backup_object):

        if os.path.exists(backup_object['path']):
            # tar+zip it in temp folder
            targz = tempfile.TemporaryFile()  # open('tmp.tar.gz', 'wb+')  #
            self._make_tarfile(output_file=targz, source=backup_object['path'])

            arch_desc = {"name": backup_object['name'], 'datetime': datetime.now(), "id": None, "encrypted": False}

            if 'encrypt' in backup_object and backup_object['encrypt']:
                # set description to encrypted
                arch_desc["encrypted"] = True

                # encrypt it
                encr = tempfile.TemporaryFile()  # open('tmp.tar.gz.enc', 'wb+')  # tempfile.TemporaryFile()
                self.crypt.encrypt(targz, encr)

            else:
                encr = targz

            self.vault.upload(fileobj=encr, arch_descr=arch_desc, print_info=True)

            targz.close()
            try:
                encr.close()
            except Exception:
                pass

        else:
            raise ConfigError(
                "backup_object '{}' has an invalid 'path' attribute. Does the file exist?".format(backup_object))

    def retrive(self, name, out_path, force, wait, archive_id=None):

        # config object is needed to get type, enkryption etc.
        backup_objects = self.vault.get_archive_list(name)
        if backup_objects is None:
            raise BackupObjectNotFound("backup_object '{}' not found in shelve".format(name))

        # get the latest element for the selected name and (if given) id
        selected_object = self.get_latest_from_dict(backup_objects, 'datetime', archive_id)[1]

        if selected_object is None:
            raise BackupObjectNotFound("backup_object '{}' not found in shelve".format(name))

        archived = tempfile.TemporaryFile()  # open('tmp_outof_archive.tar.gz.enc', 'wb+')  #
        if not self.vault.retrieve(selected_object['id'], fileobj=archived, wait_mode=wait, print_info=True):
            raise NotReadyYet("AWS Glacier job not ready yet, it takes 3 to 5 hours")

        decrypted = None
        if 'encrypted' in selected_object and selected_object['encrypted']:
            decrypted = tempfile.TemporaryFile()  # open('tmpdecr.tar.gz', 'wb+')  #
            self.crypt.decrypt(archived, decrypted)
        else:
            decrypted = archived

        # unpack file(s) and write into dest_folder
        self._extract_tarfile(output_folder=out_path, source_file=decrypted, overwrite=force)

        archived.close()
        try:
            decrypted.close()
        except Exception:
            pass

    @staticmethod
    def _make_tarfile(output_file, source):
        with tarfile.open(fileobj=output_file, mode="w:gz") as tar:
            tar.add(source, arcname=os.path.basename(source))

    @staticmethod
    def _extract_tarfile(output_folder, source_file, overwrite=False):
        # TODO Inspect tar before extract
        # Warning  Never extract archives from untrusted sources without prior inspection.
        # It is possible that files are created outside of path, e.g. members that have absolute
        # filenames starting with "/" or filenames with two dots "..".
        source_file.seek(0)

        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
        t = tarfile.open(fileobj=source_file, mode="r:gz")

        # check if files will get overwritten
        if not overwrite:
            for file in t.getnames():
                write_to = os.path.join(output_folder, file)
                if os.path.exists(write_to):
                    raise FileExistsError(write_to)

        t.extractall(output_folder)

    @staticmethod
    def get_latest_from_dict(dict_to_sort, order_attr, key_startswith=None):
        def key_from_item(func):
            return lambda item: func(*item)

        if key_startswith is not None:
            filtered_dict = {k: v for k, v in dict_to_sort.items() if k.startsWith(key_startswith)}

            use_dict = filtered_dict
        else :
            use_dict = dict_to_sort

        s = sorted(
            use_dict.items(),
            key=key_from_item(lambda k, v: (v[order_attr], k)),
            reverse=True
        )

        return s[0]


def init_argparse():
    # create the top-level parser
    parser = argparse.ArgumentParser(prog='AmazonGlacierBackup')
    parser.add_argument('-c', dest='conf_file', help='Path to alternative config file', default=None)
    subparsers = parser.add_subparsers(dest='mode')

    # create the parser for the "backup" command
    parser_a = subparsers.add_parser('backup',
                                     help='Archives the object with the given name configured in the config file in amazon glacier')
    parser_a.add_argument('-name', help='object name')

    parser_d = subparsers.add_parser('backuponce',
                                     help='Archives the given file/folder in amazon glacier')
    parser_d.add_argument('name', help='Name to use for the glacier archive')
    parser_d.add_argument('file', help='File or folder to archive')
    parser_d.add_argument('-encrypt', action='store_true', dest='encrypt')

    # create the parser for the "get" command
    parser_b = subparsers.add_parser('get', help='Initiates the retrival of the file from amazon glacier')
    parser_b.add_argument('name', help='Archive name')
    parser_b.add_argument('out', help='Folder to save the restored object in')
    parser_b.add_argument('-id', help='Archive id, can be used to restore a specified version instead of the newest',
                          dest='id')
    parser_b.add_argument('-wait', action='store_true', dest='wait')
    parser_b.add_argument('-force', action='store_true', dest='force', help='Overwrite existing files')

    parser_c = subparsers.add_parser('list', help='Show a list of all archived objects.')
    parser_c.add_argument('-a', action='store_true', help='Show with versions of objects', dest='all')

    return parser


# def main():
#
#     if arg.mode == 'archive':
#
#         # todo test is file -> print message
#         arch_file = open(arg.filename, "rb")
#         gv.upload(fileobj=arch_file, arch_descr=arch_desc, print_info=True)
#         arch_file.close()
#
#     elif arg.mode == 'retrive':
#         arch_file = open(arg.filename, "wb")
#         arg.archive_name
#
#         gv.retrieve(fileobj=arch_file, archive_id=archive_id, wait_mode=arg.wait, print_info=True)
#         arch_file.close()
#     else:
#         parser.print_help()


def main():
    parser = init_argparse()
    arg = parser.parse_args()

    if arg.conf_file is None:
        agb = Agbackup('config.json')
    else:
        agb = Agbackup(arg.conf_file)

    if arg.mode == 'backup':
        agb.backup(arg.name)

    elif arg.mode == 'backuponce':
        temp_backup_data = {
            "path": arg.file,
            "name": arg.name,
            "encrypt": arg.encrypt
        }
        agb.backup_element(temp_backup_data)

    elif arg.mode == 'get':
        agb.retrive(arg.name, arg.out, arg.force, arg.wait, arg.id)

    elif arg.mode == 'list':
        arlist = agb.vault.get_archive_list()
        for name, arch_obj in arlist.items():
            print('{}'.format(name))
            if arg.all:
                for arch_id, arch_data in arch_obj.items():
                    print('\t {dt}: {id} Enrypted: {enc}'.format(dt=arch_data['datetime'], id=arch_id,
                                                                    enc=arch_data['encrypted']))

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
