{ nixpkgs ? import <nixpkgs> {}, compiler ? "default" }:

let

  inherit (nixpkgs) pkgs;

  f = { mkDerivation, async, base, bytestring, exceptions, mtl
      , pipes, pipes-concurrency, pipes-safe, postgresql-simple, stdenv
      , stm, text, transformers
      }:
      mkDerivation {
        pname = "pipes-postgresql-simple";
        version = "0.1.3.0";
        src = ./.;
        libraryHaskellDepends = [
          async base bytestring exceptions mtl pipes pipes-concurrency
          pipes-safe postgresql-simple stm text transformers
        ];
        description = "Convert various postgresql-simple calls to work with pipes";
        license = stdenv.lib.licenses.mit;
      };

  haskellPackages = if compiler == "default"
                       then pkgs.haskellPackages
                       else pkgs.haskell.packages.${compiler};

  drv = haskellPackages.callPackage f {};

in

  if pkgs.lib.inNixShell then drv.env else drv
