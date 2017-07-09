import numpy as np
import sys

def symmetrize(a):
    return a + a.T - np.diag(a.diagonal())

if __name__ == '__main__':
    tf_matrix = np.loadtxt(
        sys.argv[1],
        dtype=np.int64
    )
    tf_matrix = symmetrize(tf_matrix)
    np.savetxt(
        sys.argv[2],
        tf_matrix,
        fmt='%d',
        delimiter='\t'
    )
               
            
