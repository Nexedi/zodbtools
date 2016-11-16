=========================================
 Zodbtools - handy set of ZODB utilities
=========================================

This repository provides a set of handy ZODB utilities. We initially tried to
put them into ZODB itself, but Jim Fulton adviced__ not to load ZODB with
scripts anymore. So we are here:

__ https://github.com/zopefoundation/ZODB/pull/128#issuecomment-260970932

- `zodbcmp` - compare content of two ZODB databases bit-to-bit.
- `zodbdump` - dump content of a ZODB database.
