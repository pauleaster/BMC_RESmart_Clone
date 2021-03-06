# BMC_RESmart

Cloned from https://github.com/headrotor/BMC_RESmart, 
see this repo for the original source.

I am using this to make a Rust port for my own use.

This is my rust port: https://github.com/pauleaster/bmc_cpap

## results:
Both the python and rust code read the binary files ~123MB (on disk) and writes a csv file of ~27MB.
The python code takes ~30 sec and the rust code takes ~1.5 sec.

## The Following is the readme from the original repo

Decode data files from BMC Medical RESMart GII systems.

Note: the operation of this code has been reverse-engineered from
undocumented data. Data outputs may be misunderstood and/or
erroneously interpreted.

WARNING: UNFIT FOR ANY MEDICAL USE INCLUDING BUT NOT LIMITED TO
DIAGNOSIS AND MONITORING. NO WARRANTY EXPRESS OR IMPLIED. UNDOCUMENTED
DATA INTERFACE: USE AT OWN RISK

The RESMart II system records and stores operational data on an SD
card. This data is also analyzed and interpreted by the RESmart nPAP
Data Analysis Software also made by BMC. Since this is not freely
available, I have created this software as an alternative.

Thsi software extracts raw data for your own analysis, including time
and duration of use, IPAP, EPAP, and Reslex pressure settings,
airflow, tidal volume, respiration rate, as well as SP02 and pulse
rate if the auxillary pulse oximeter is used.

Note that the determination of apnea/hypopnea events seems to be done
only in the BMC software and not on the device, so this is not
currently supported by the software here.

----

The RESmart GII systems record operational data on an SD card. To extract this data, copy the files using an SD card into your own computer and run the python program.

The files will be in the form:

~~~~

NNCNNNNN.evt
NNCNNNNN.idx
NNCNNNNN.usr
NNCNNNNN.log
NNCNNNNN.000
NNCNNNNN.001
.
.
.
~~~~

The NNCNNNN file root is the device serial number, for example '16C01034'

Raw data from the device is stored in the files with numerical
extensions. It's my guess that this is analyzed by the BMC software
and results are stored in the .usr and .evt files, but without
confirmation this is conjecture.  User information such as name and
address is stored in the .usr file, while the .log file seems to store
interactions with the software.

The code here reads only the raw data files. Run in the same directory, it looks for raw data files of the form *.nnn where nnn is a three-digit integer. These are read in sequence.

Each file consists of a sequence of 256-byte packets, Each packet
corresponds to one second of data formatted as 120 two-byte unsigned
integers followed by an 8-byte timestamp. The format of the integers
is as follows (these are guesses!):

~~~~

Address      Interpretatation
000          Always 0xAAAA
001          Reslex value (1-5)
002          IPAP value in units of 0.5 cm H20 (divide by 2 to get cm)
003          EPAP value in units of 0.5 cm H20 (divide by 2 to get cm)
004-028      25 values of something related to pressure at 25 Hz
029-053      25 values of of something related to pressure at 25 Hz
054-078      25 values of instaneous flow at 25 Hz
079-084      10 values of something oscillatory at 10Hz (motor drive?)
085-094      unknown values related to pressure?
095          Tidal volume in liters per minute
098          spO2 (blood oxygenation) in percent (only if oximeter attached)
099          Heart rate in bpm (only if oximeter attached)
100          Respiration rate in breaths per minute
101-120      zero padding
~~~~

Some values are 0xffff (65535) when not valid, for
example the respiration rate takes 30 or more seconds to become valid
after the start of pressure flow.

The last 8 bytes are the timestamp, one 16-bit integer for the year,
followed by 5 unsigned bytes for month, day, hour, minute, and
second. The final byte is an unknown value.


~~~~

usage: python resmart_parse.py
[in a directory containing .000, .001... raw data files ]

positional arguments:
  output_file         Output data CSV file

optional arguments:
  -h, --help          show this help message and exit
  --list_dates, -l    Print dates found in stored data. Suppresses other
                      output
  --time_ymd, -y      Print timestamp in Y, M, D, H, M, S format
  --time_seconds, -s  Print timestamp in seconds since beginning of month
  --dates, -d,       Select date range in YYYY-MM-DD format. Single date is one day, two dates are start and end of time range.
  --quiet, -q         Do not print progress and info to stderr
~~~~

