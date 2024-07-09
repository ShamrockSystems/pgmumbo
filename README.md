- [ ] Build heap scanner
    - [ ] Figure out heap relation structure
    - [ ] Determine primary key
    - [ ] Convert row to milli document
    - [ ] Setup scanning function
    - [ ] Setup scanning callback
- [ ] Transaction hooks
    - [ ] add rows
    - [ ] remove rows
    - [ ] add columns
    - [ ] remove columns
- [ ] Transaction abort callback

Create canonical naming system for milli indices per namespaced OID. Allow multiple postgres indices to act on a single milli index.

I think like Zombo it'd be most wise to set up something like the executor manager where we build up a state of documents, and then at the end of the scan apply all of that to the milli index.
