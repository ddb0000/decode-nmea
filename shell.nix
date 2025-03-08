{ pkgs ? import <nixpkgs> {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    (python312.withPackages (ps: with ps; [
      pandas
      numpy
      pyais
      pyarrow
      matplotlib
      jupyterlab
      ipykernel
      ipywidgets
      bitstring
      folium
      seaborn
      plotly
      dash
      folium
    ]))
  ];

  shellHook = ''
    echo "Nix shell with Python 3.12, dependencies, and Jupyter ready!"
  '';
}