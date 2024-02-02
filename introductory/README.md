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

To build the plots and tables in the paper, you will need to install R and a few
R packages. The R packages can be installed in an R session with:

```r
install.packages(c("gt", "ggplot2", "tidyr", "dplyr", "rmarkdown", "reticulate"))
```

### Troubleshooting
On MacOS when you install the R packages, if you see an error like:

`'/opt/X11/lib/libSM.6.dylib' (no such file)`

Just reinstall [XQuartz](https://www.xquartz.org/)

## Building the paper

The paper can be built with:

```shell
cd paper
make
```
