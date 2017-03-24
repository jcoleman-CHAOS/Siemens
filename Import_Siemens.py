from influxdb import InfluxDBClient
from pylab import *
from datetime import datetime
import pytz
import credentials as cd
from os import walk, getcwd, rename, path, mkdir

current_directory = getcwd()

# use local InfluxDB credentials
usr = cd.usr
password = cd.passwd
db = 'siemens'  # cd.db
measurement = 'siemens'
host = cd.host
port = cd.port
client = InfluxDBClient(host, port, usr, password, db)

# set up timezones
utc = pytz.utc
eastern = pytz.timezone('US/Eastern')
fmt = '%Y-%m-%dT%H:%M:%S.0000000Z'  # in nanoseconds


def get_config_settings():
    _config = {}
    s = open("siemens_importer.config", 'r')
    for _line in s:
        if "=" in _line:
            _x = _line.split("=")
            _config[_x[0]] = _x[1]
    # if no file_suffix provided, use default value
    try:
        _config['file_suffix']
    except KeyError:
        _config['file_suffix'] = 'siemens_log.csv'
    return _config

config = get_config_settings()  # Generate configuration settings dict


class Folder():
    def __init__(self):
        self.dirpath = []
        self.dirnames = []
        self.filenames = []

        self.select_files = []

        self.gen_results()

    def gen_results(self):
        for (dirpath, dirnames, filenames) in walk(current_directory):
            # Load response from walk
            self.dirpath.extend(dirpath)
            self.dirpath = "".join(self.dirpath)
            self.filenames.extend(filenames)
            self.dirnames.extend(dirnames)
            break

        # remove hidden
        self.filenames = [i for i in self.filenames if i[0] != "."]
        self.dirnames = [i for i in self.dirnames if i[0] != "."]

    def update(self):
        self.dirpath = []
        self.dirnames = []
        self.filenames = []

        self.gen_results()

    def selective_suffix(self):
        self.update()
        self.select_files = [_f for _f in self.filenames if config['file_suffix'] in _f]
        return self.select_files


# GLOBAL FUNCTIONS
# This is to put data in the correct format
def json_write(_measurement, _name, _location, _time, _value):
    json2write = [
        {
            "measurement": _measurement,
            "tags": {
                'name': _name,
                'location': _location
            },
            "time": _time,
            "fields": {
                "value": _value,
            }
        }
    ]
    return json2write


# checks for number-ness
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False


# handling data types
def siemens_value(raw_value, measure='Unknown Measurement'):
    val_map = {
        'ON': 1.0,
        'OFF': 0.0
    }
    if is_number(raw_value):
        result = float(raw_value)
        return result
    else:
        if raw_value in val_map:
            result = val_map[raw_value]
            return result
        else:
            print 'ERROR, unknown reading from  ' + measure


# BEGIN - Collecting files and process them
print 'Beginning import'
folder = Folder()
for _file in folder.selective_suffix():
    # load file
    a = []  # this holds the files data
    f = open(_file, 'r')
    for line in f:
        a.append(line)
    f.close()

    # list comprehension for cleaning the file
    clean_file = [line[:-2] for line in a if len(line) > 3]  # Removes unwanted chars and lines
    label = ''  # active variable
    location = ''  # active location

    # Generate JSON strings from each line measurements
    for index, l in enumerate(clean_file):
        # IF new variable: update the label variable and location
        if len(l) > 0 and l.split()[0] == "Point":
            tags = l.split()[2].split(".")
            location = tags[0]
            label = ''.join(tags[1:])
        # IF data line is found... create JSON
        if len(l) == 54:
            # SPLIT input string
            y = l.split()
            value = siemens_value(y[2], label)  # modify the value to the correct type

            # data and time
            d = y[0].split('/')
            d = map(int, d)
            t = y[1].split(":")
            t = map(int, t)
            loc_dt = eastern.localize(datetime(d[2], d[0], d[1], t[0], t[1], t[2]))

            # generate JSON, ADD to Influxdb
            entry = json_write(measurement, label, location, loc_dt, value)
            client.write_points(entry)

        if index % 10 == 0:
            print "\r.",
        else:
            print ".",

    print "\nImported: " + _file
    if not path.exists(current_directory + '/ARCHIVE'):
        mkdir(current_directory + '/ARCHIVE')
    rename(_file, current_directory + '/ARCHIVE/' + _file)