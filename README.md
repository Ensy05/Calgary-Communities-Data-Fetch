This python file compiles Immigration & Non-Immigration population makeup by community in Calgary:
https://www.calgary.ca/communities/profiles.html

./community-names.txt is a copy and paste of the list of communities on the above page

Import Info:
- _multiprocessing_ and _ThreadPoolExecuter_ from _concurent.futures_ are both unnecessary and only used to fetch and compile multiple PDFs in parallel

WIP:
- currently, collecting data from every table is a WIP