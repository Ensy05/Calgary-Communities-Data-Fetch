This python file compiles Immigration & Non-Immigration population makeup by community in Calgary:
https://www.calgary.ca/communities/profiles.html

./community-names.txt is a copy and paste of the list of communities on the above page

Import Info:
- _multiprocessing_ and _ThreadPoolExecuter_ from _concurent.futures_ are both unnecessary for workign code but act fetch and compile multiple PDFs in parallel.

WIP:
- collecting data from every PDF table
- configuration of required data
- optimizing to minimize required storage capacity

Notes:
- I really appreciate comments on any suggestions / bugs
- This code is highly dependant on **Internet** and **Storage** Speeds. Data must be downloaded as a PDF before compiling, ~300MB of temporary downloads can be expected.