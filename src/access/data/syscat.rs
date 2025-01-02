
/* stores:


1. Table name
2. Table ID
3. column details
    a. type of data
    b. range of data (probably won't be used, all sizes are static)
    c. required (allows NULLs or not)
    d. key
        i. if foreign, which table it relates to
3. index details
    a. type
    b. columns indexed
4. storage data
    a. page ids
    b. number of rows


*/