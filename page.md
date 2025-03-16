### Pages should allow:

- [X] inserting a record
    - [X] Check a freelist to see if theres any open slots. If so, insert into the first one, and set the head of the freelist to be the value the current spot was pointing to
    - [X] If the freelist is not started, then find the end of the page and insert there
- [X] removing a record
    - [X] if the record ID being removed is at the end, remove it and decrement the endofpagepointer
    - [X] otherwise, remove the record and set the last value of the freelist to point to this new spot


### Table interactions:
- [X] Inserting a record
    - [X] check if there are any pages. If not, create one and insert into it
    - [X] if there are pages, get the first value in the system catalogs free pages list, and insert into its free spot
        - [X] check if the page still has a free space (freelist is empty or end of page can't fit any more records). If not, remove it from the free list
    - [X] if there are no free pages, create a new one and insert into it
- [ ] removing a record
    - [ ] find the page that the record belongs to and remove it. If that page is not already on the free list, add it  



***NOTE***: The record IDs must not reset for each new page. That is, record IDs are *unique*, use a u32 i think


### what to add to syscat:
- [X] next record id
- [X] free page id list
