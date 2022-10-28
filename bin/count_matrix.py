import os
import sys
import numpy as np
import pandas as pd

def convert_matrix(data_dir):
    # data_dir = "D:/download/0.5Result"
    files = os.listdir(data_dir)

    ## cell_row
    row_dict = {}

    ## gene_col
    col_dict = {}

    gene_list = []
    cell_list = []
    count_list = []

    for file in files:
        if not os.path.isdir(file):
            f = open(data_dir + "/" + file)
            iter_f = iter(f)
            for line in iter_f:
                gene, cell, count = line.split("\t")
                gene_list.append(gene)
                cell_list.append(cell)
                count_list.append(int(count))

    gene_list = np.array(gene_list)
    cell_list = np.array(cell_list)
    count_list = np.array(count_list)

    col_list = np.unique(gene_list)
    row_list = np.unique(cell_list)

    for i in range(len(col_list)):
        col_dict[col_list[i]] = i
    for i in range(len(row_list)):
        row_dict[row_list[i]] = i

    matrix = np.zeros((len(row_list), len(col_list)))

    for i in range(len(gene_list)):
        gene_index = col_dict[gene_list[i]]
        cell_index = row_dict[cell_list[i]]
        count = count_list[i]
        matrix[cell_index][gene_index] += count

    matrix_pd = pd.DataFrame(matrix)
    matrix_pd.index = row_list
    matrix_pd.columns = col_list
    matrix_pd.to_csv(data_dir + "/count.csv")

# if __name__ == '__main__':
convert_matrix(sys.argv[1])
