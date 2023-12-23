# Lance

This paper contains all assets for the introductory paper.

## Structure

* `experiments/` contains all experiments (code and data) shown in the paper.
* `paper/` contains the Quarto source code for the paper.

## Project setup

This project is based on the [ACM template](https://github.com/mikemahoney218/quarto-arxiv).
You will need to install Quarto as well as some Tex installation. Quarto can be
installed from the website. On Mac, you can install [MacTex](https://www.tug.org/mactex/)

```shell
brew install quarto
```

There are some fonts that need to be installed. On Mac, this can be done with:

```shell
brew tap homebrew/cask-fonts
brew install font-latin-modern
brew install font-latin-modern-math
```

## Building the paper

The paper can be built with:

```shell
cd paper
make
```
