{ pkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/nixos-23.05.tar.gz") {} }:

pkgs.mkShell {
  buildInputs = [
    pkgs.python310Full
    pkgs.python310Packages.virtualenv
    pkgs.python310Packages.fastapi
    pkgs.python310Packages.uvicorn
    pkgs.python310Packages.redis
    pkgs.redis
    pkgs.python310Packages.requests
    pkgs.python310Packages.httpx
    pkgs.python310Packages.python-dotenv
    pkgs.python310Packages.yfinance
    pkgs.python310Packages.pydantic
    pkgs.python310Packages.prometheus_client
    pkgs.python310Packages.rq
    pkgs.python310Packages.spacy
  ];
}
