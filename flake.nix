{
  description = "Development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in {
        # mkShell, not mkShellNoCC: go test -race needs cgo, so a C compiler.
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            go
            git
            gh
            buf
            protoc-gen-go
          ];
        };
      }
    );
}
