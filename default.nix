{ mkDerivation, async, base, bytestring, exceptions, mtl, pipes
, pipes-concurrency, pipes-safe, postgresql-simple, stdenv, stm
, text, transformers
}:
mkDerivation {
  pname = "pipes-postgresql-simple";
  version = "0.1.2.0";
  src = ./.;
  buildDepends = [
    async base bytestring exceptions mtl pipes pipes-concurrency
    pipes-safe postgresql-simple stm text transformers
  ];
  description = "Convert various postgresql-simple calls to work with pipes";
  license = stdenv.lib.licenses.mit;
}
