# Column-Oriented Store with Scan Optimizations
## Overview

This project implements a column-oriented storage system with several scan optimization techniques to improve query performance. The enhancements include:

Compression – Reduces storage size and improves cache efficiency
Sorting – Organizes data to enable faster scans
Zone Maps – Skips irrelevant data blocks during queries
Shared Scanning – Reuses scans across multiple queries
Caching – Stores intermediate results to avoid redundant computation

These techniques work together to significantly reduce I/O and improve query execution speed.

## How to Run

To run the program, execute the following command in your terminal:

```
python main.py
```

Make sure you have Python installed and that you are in the project directory before running the command.
