import os
import struct
import sys
import glob
import argparse
import datetime
import time

start = time.time()


class packet(object):
    """ Holds data for one 256-byte packet from the raw (numerical
    extension) RESmart data files. Also methods for parsing the data """

    def __init__(self,start, pbuf):
        """ given pbuf list of bytes, parse out the data"""

        assert len(pbuf) >= 256

        # length of data fields (uint16_t), not counting zero pads and timestamp
        self.dlen = 106

        # extract timestamp
        self.parse_timestamp(pbuf)

        # extract data fields
        self.parse_data(pbuf)

        # generate human-readable labels
        self.setup_labels()

        # if self.data[self.known_fields["spO2_pct"]] > 0:
        #     self.has_pulse = True
        # else:
        self.has_pulse = False
        

    def setup_labels(self):
        """ These are human-readable labels for the headers of .csv files"""
        # set up data structure for field labels
        self.timestamp_fields = ["year","month","day","hour","minute","second","?"]
        # default label is "?" for unknown data field
        self.data_fields = ["?" for i in range(self.dlen)]
        
                
        # some data fields are known, label them
        self.known_fields = {
            "Reslex":1,
            "IPAP":2,
            "EPAP":3,
            "tidal_vol":99,
            "rep_rate":104}
        
        for key, val in self.known_fields.items():
            self.data_fields[val] = key
    
    def parse_timestamp(self,pbuf):
        """ the timestamp is the last 8 bytes of every packet"""
        self.timestamp = struct.unpack("HBBBBBB",pbuf[256-8:256])

        # for readability and convenience, parse out individual fields.
        self.year = self.timestamp[0]
        self.month = self.timestamp[1]
        self.day = self.timestamp[2]
        self.hour = int(self.timestamp[3])
        self.minute = self.timestamp[4]
        self.second = self.timestamp[5]
        
        self.date = datetime.date(self.year,self.month,self.day)
        self.ordinal = self.date.toordinal()
        self.datestr = self.date.isoformat()

    def parse_data(self, pbuf):
        """  extract all 16-bit uint16_t data (dlen words) from the packet"""
        self.data = []
        assert 2*self.dlen < len(pbuf)
        for i in range(self.dlen):
            ptr = 2*i # uint16_t, 2-byte unsigned integers
            val = struct.unpack("H",pbuf[ptr:ptr+2])
            self.data.append(val[0])

    def get_known_header_csv(self):
        # text descriptions of known values
        outstr = ""
        for key, val in self.known_fields.items():
            outstr += "{}, ".format(key)
        return outstr


    def get_known_values_csv(self):
        # print only understood values
        outstr = ""
        for key, val in self.known_fields.items():
            outstr += "{}, ".format(self.data[val])
        return outstr


    def fix_csv(self, csv_str):
        # remove trailing spaces & comma, add newline 
        csv_str = csv_str.strip()
        if csv_str[-1] == ',':
            csv_str = csv_str[0:-1]
        return csv_str 

    def get_time_ymd_header_csv(self):
        # return time string in year, month, day format
        return ",".join(self.timestamp_fields[:-1])

    def get_time_ymd_csv(self):
        # return time string in year, month, day format
        outstr = ""
        for i in self.timestamp[:-1]:
            outstr += "{:d}, ".format(i)
            
        return outstr

    def get_time_seconds_header_csv(self):
        return "seconds"
            
    def get_time_seconds(self):
        return self.second + 60*self.minute + 3600*(self.hour + 24*(self.ordinal))








if sys.version_info.major < 3:
    print("sorry, requires Python 3.")
    exit(1)


parser = argparse.ArgumentParser(
    description='Extract data from BMC RESmart raw data files')

parser.add_argument('--time_ymd','-y',
                    action='store_true',
                    help='Print timestamp in Y, M, D, H, M, S format')

parser.add_argument('--time_seconds','-s',
                    action='store_true',
                    help='Print timestamp in seconds since beginning of year')

parser.add_argument('--quiet','-q',
                    action='store_true',
                    help='Do not print progress and info to stderr')

parser.add_argument('output_file', nargs = '?', 
                    help='Output data CSV file, careful will overwrite existing data.',
                    default="RESmart_data.csv")

parser.add_argument('--dates', '-d',  nargs = '+', 
                    help='Select date range in YYYY-MM-DD format. Single date is one day, two dates are start and end of time range.',
                    default=[])

parser.add_argument('--path', '-p', 
                    help='Enter the path for the CPAP data files.', 
                    default=".")

args = parser.parse_args()



start_date = None
end_date = None
if len(args.dates) > 0:
    try:
        start_date = datetime.datetime.strptime(args.dates[0], '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Incorrect -d date format, should be YYYY-MM-DD")
        exit()

if len(args.dates) > 1:
    try:
        end_date = datetime.datetime.strptime(args.dates[1], '%Y-%m-%d').date()
    except ValueError:
        raise ValueError("Incorrect -d date format, should be YYYY-MM-DD")
        exit()

if len(args.path) > 0:
    path = args.path
    path_exists = False
    try:
        path_exists = os.path.isdir(path)

    except ValueError:
        raise ValueError("Incorrect path supplied")
        exit()
    if not path_exists:
        raise ValueError("Incorrect path supplied")
        exit()



# Should probably ensure these files all have the same root...
filesNNN = glob.glob(os.path.join(path,'*.[0-9][0-9][0-9]'))
filesNNN.sort()

# print(path,'\n', filesNNN)



packets = []
thispacket = None
packetsize = 256


for datafile in filesNNN:
    with  open(datafile, "rb") as f:
        databuff = f.read()

    oldday = -1
    p = 0 # pointer into byte array
    while p < (len(databuff) - packetsize):
        #print(i)

        val = int(struct.unpack("H",databuff[p:p+2])[0])
        thispacket = packet(p, databuff[p:p+packetsize])
        p += packetsize
        packets.append(thispacket)

        if thispacket.day != oldday and not args.quiet:
            if thispacket.date == start_date:
                print("Found start date {}.".format(start_date.isoformat()))
            oldday = thispacket.day
            print("reading data from {}".format(thispacket.datestr))

if not args.quiet:
    print("{:d} packets found in {} files".format(len(packets), len(filesNNN)))
 



if start_date is None:
    #default to beginning of data
    start_date = packets[0].date
else:
    if end_date is None:
        # if only start date is specified, use only that date
        end_date = start_date

if end_date is None:
    #default to end of data
    end_date = packets[-1].date 

#print(start_date)
#print(end_date)

first_header_row = True

with open(args.output_file, 'w') as outf:
    # read data 
    
    day = -1
    for i, p in enumerate(packets):
        if i == 0 and first_header_row:
            first_header_row = False
            outstr = ""
            if args.time_seconds:
                outstr += "{}, ".format(p.get_time_seconds_header_csv()) # change to header

            if args.time_ymd:
                outstr += p.get_time_ymd_header_csv() + ','

            outstr += p.get_known_header_csv()
            outf.write(p.fix_csv(outstr) + "\n")   

        if p.date >= start_date and p.date <= end_date:

            if p.ordinal != day and not args.quiet:
                day = p.ordinal
                print("Writing {} data to {}".format(p.datestr,
                                                     args.output_file))

            outstr = ""
            if args.time_seconds:
                outstr += "{}, ".format(p.get_time_seconds())

            if args.time_ymd:
                outstr += p.get_time_ymd_csv()

            outstr += p.get_known_values_csv()
            csv_str = p.fix_csv(outstr) + "\n"
            outf.write(csv_str)
end = time.time()
print("Elapsed time = {:0.3f}".format(end - start))      






