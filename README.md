# MSD Lyrics SQL database 

A command line tool to load the lyrics subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) 
into an [H2 SQL database](http://www.h2database.com/html/main.html).

A SQL database makes it easy to inspect, clean, aggregate, filter and slice the dataset, via a GUI or programmatically.
 
 
## Installation

 
Install [Java](http://java.com/en/download/), download the jar file from the release page of this repository and follow the instructions below. 

Alternatively, you can clone this repository and run the code with [Leiningen](http://leiningen.org/), the build automation tool 
for [Clojure](http://clojure.org). Start by editing the `msd.edn` file in the project root (see further below) and then execute 
the code with `lein run`.

You don't need to install H2 on your machine to run the program. The database engine is embedded in the program. 

However, you'll need some H2 compatible tool to view the data. An option is to [install H2](http://www.h2database.com/html/download.html) 
and use the included [console application](http://www.h2database.com/html/quickstart.html).
Another is to use an H2 compatible front-end such as [DataGrip](https://www.jetbrains.com/datagrip/).




## Usage

Gather the following files from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) and place them in 
a same directory (while you're browsing the websites, check the licensing/citing terms for the various subsets):

- from https://labrosa.ee.columbia.edu/millionsong/musixmatch:
  - mxm_779k_matches.txt
  - mxm_dataset_test.txt
  - mxm_dataset_train.txt
  - mxm_reverse_mapping.txt
- from https://labrosa.ee.columbia.edu/millionsong/pages/getting-dataset:   
  - tracks_per_year.txt
- from https://labrosa.ee.columbia.edu/millionsong/blog/11-3-15-921810-song-dataset-duplicates
  - msd_duplicates.txt
- from https://labrosa.ee.columbia.edu/millionsong/secondhand
  - shs_dataset_test.txt
  - shs_dataset_train.txt
- from http://www.ifs.tuwien.ac.at/mir/msd/download.html#groundtruth: 
  - msd-MASD-styleAssignment.cls

Place the jar file into the same directory and run:

    $ java -jar msd-to-h2-0.2.1-standalone.jar

Give it a few minutes to create the output files:

    Creating the csv files...
    Creating the database. Just a bit of patience.
    Creating primary tables...
    Creating indexes...
    Creating derived tables...
    All done!
    The 3 artists with the largest vocabulary in the Million Song Dataset are Aesop Rock with 2555 words, Eminem with 2526 words, Cypress Hill with 2476 words




## Outputs

The program runs in two stages. 

Firsty, the program converts the original MSD files into CSV files. Words and tracks are given new unique integer ids, 
and files that relate to each others are consolidated (e.g. tracks + track years + track genres).
  
Secondly, the program uploads the resulting CSV files into a new H2 database. Tables are created for tracks, words and the track/word matrix. 
The tool also creates a table of artists (based  on the MusicXMatch artist names in the dataset) with aggregate track count, vocabulary count, 
and year range for each artist. This list of artists is preliminary and is meant to help prioritize data cleaning, rather than being used as is. 




## Options

Rather than having all input and output files in the same directory, it is possible to specify different locations for input files, csv outputs files 
and the database. To do this, create an [edn file](https://learnxinyminutes.com/docs/edn/) called `msd.edn` with the following keys:

- `:in`  directory of the MSD input files 
- `:csv` directory of the csv output files
- `:db` output database file according to 
[H2's URL format for an embedded database](http://www.h2database.com/html/features.html#database_url), without the `jdbc:h2:` prefix. 
The code was tested for a location relative to the `msd.edn` file (`./`) 
or to the user's home directory (`~/`).

For example: 
     
     {:in   "/data/msd/source/" 
      :csv  "/data/msd/csv/" 
      :db   "~/h2data/msd"}
 
You can then run `java -jar msd-to-h2-0.2.1-standalone.jar` at the location of the `msd.edn` file.

If there is no `msd.edn` file (like earlier) then the program defaults to the following parameters:

    {:in   "./" 
     :csv  "./" 
     :db   "./msd"}




## Other SQL Engines

Amending the code to accommodate other SQL engines should be straightforward
. 
You'll have to:

- [ ] change the database driver dependency in `project.clj` 
- [ ] adapt the database spec in `src/intoh2/core.clj` (look for`db-spec` and `create-database`)
- [ ] adapt the scripts in `resources/sql` to the SQL dialect of the new database  




## License

Copyright © 2018 Nicolas Duchenne, Belove Ltd, London, UK

Released under the [MIT License](https://opensource.org/licenses/MIT).
