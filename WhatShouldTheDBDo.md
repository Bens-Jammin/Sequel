# What Should The DataBase Be Able To Do?


## Storage
- Capable of storing the following datatypes:
    - [ ] Strings
    - [ ] numbers
    - [ ] dates / times
    - [ ] booleans
    - [ ] null values
    - [ ] links
- store in binary files (.bin extension) in a designated folder


## Features
- [ ] short term caching for easy access
- [ ] indexing
- [ ] faster file I/O
- [ ] improved CRUD operation speed
- [ ] improved db access syntax / language


## Error Handling
- Throw errors on the following: 
    - [ ] invalid data type
    - [ ] null value not permitted
    - [ ] inserting/editing/removing from a nonexistent column
    - [ ] adding a key that already exists
- log errors in a text file ?
- display errors (along with potential reason and fix) to user