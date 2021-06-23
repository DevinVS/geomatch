# geomatch
Utility for fetching and matching csv files


## Installation
Download the latest version of the application for your operating system [from here](https://github.com/DevinVS/geomatch/releases/)

On MacOS or Linux, test inside a terminal window
```bash
./geomatch_linux_64-bit.sh -V
```

or on Windows inside a Command Prompt
```batch
geomatch_windows_64-bit.exe -V
```

## General Usage

In order to run the application you must supply as arguments your api key for google's geocoding service along with the csv files for the application to use.
The api key can either be supplied as a command line argument -k or be set as the environment variable `API_KEY`, whichever is more convenient.

So, basic usage would look like this:
```bash
./geomatch_linux_64-bit.sh -k 'API_KEY_GOES_HERE' file1.csv file2.csv file3.csv ...
```

Once the application is running you will be presented with a cli interface with some basic commands:

- `list [index]`
  + List all the columns for the csv file at a specific index (starting at 0)
- `config`
  + Print out the current configuration
- `set [index] [var] [col]`
  + Assign a column to a value in the configuration
- `add [index] [type] [col]`
  + Add a column to either compare or output for the matching process
- `prefix [index] [val]`
  + Set a prefix for all columns from file at a specific index
- `method [method]`
  + Set matching either to `left` for a left join or `inner` for an inner join. Outer join hopefully coming soon.
- `fetch`
  + Fetch all the coordinate pairs for all files and write to new csv files
- `match`
  + Match all the files together and output to `matches.csv`
- `quit`
  + Exit the application
- `help`
  + print a help message

## Fetching

In order to fetch latitude/longitude pairs for an address, you have to make sure all the necessary variables are set in the config. You can check the config by typing the `config` command.
`addr1`, `city`, `state`, and `zipcode` are required. The application will try to guess correct values for these, but manually setting them is sometimes required.
For instance, if you wanted to set the addr1 variable for the first file to a column named "Street_Address", the following command would suffice
```
geomatch> set addr1 0 Street_Address
```
You can find all column names using the `list` command.

Once all variables are set, you can run the fetch command.
```
geomatch> fetch
```

This will fetch all the pairs from the google api and output new csv files in your current directory.

## Matching

NOTE: currently matching only works with 2 files. If you enter more than 2 files into the arguments additional files will be ignored.

In order to match, only variable `lat` and `lng` are required. You can set them in a similar fashion to the variables seen above.
Set your match method using the `method` command. There are currently 2 methods:

- left
  + All entries from the leftmost file are written along with any matches from other files
- inner
  + Only matches are written

You can set output columns, or the columns that will be written along with the matches, using the add command:
```
geomatch> add 0 output Street_Address
```

If you want a prefix for the output columns on a per-file basis, you can set it using the prefix command:
```
geomatch> prefix 0 lek
```

In this scenario, the name of the output column would be `lek_Street_Address`

Once you are satisfied with the configuration, run the matching program with the match command:
```
geomatch> match
```
And you can find the final output file as `matches.csv` in your current working directory.

