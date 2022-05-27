# Analysing Twitter during the Ukraine-Russia conflict
This repository contains a project made during the Spring Semester 2022 at the University of Fribourg, in the *Social Media Analytics* course. The goal was to perform a community detection on Twitter networks related to the Ukraine-Russia conflict. These networks contain users (nodes) linked to each other (edges) if they liked or retweeted a tweet. The original dataset can be found [here](https://www.kaggle.com/datasets/bwandowando/ukraine-russian-crisis-twitter-dataset-1-2-m-rows).

The *Pipfile* and *Pipfile.lock* files are given to reproduce the experiments and run the code. One can install all needed dependencies by using the `pipenv install` command inside the repository directory.

To run the *Jupyter Notebooks*, the command `pipenv run jupyter notebook` can then be used.<br>
**N.B.** Due to Twitter API rate download restrictions, the notebook 1 took a long time to run (i.e. approx. 1 week for the complete data gathering).