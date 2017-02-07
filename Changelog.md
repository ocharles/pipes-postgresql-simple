# 0.1.3.0

* Fix a major bug in `fromTable`, which prevented it from ever working. Thanks to 
  @jb55 for this fix.

* This library is now deprecated in favour of `streaming-postgresql-simple` and
  will only be receiving bug fixes and Cabal file changes.

# 0.1.2.0

* Add `Pipes.PostgreSQL.Simple.query_`.

# 0.1.1.2

* Increased lower bound of `pipes` to 4.0. Thanks to Gabriel for fixing this and
  @pradermecker for initially reporting this.

# 0.1.1.1

* This release simply changes the cabal file to include the `Source-Repository`
  directive. Thanks to Patrick Wheeler (@Davorak) for noticing and correcting
  this omission.
