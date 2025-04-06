import numpy as np

def get_shares_from_dictionary(d):
	cdef int idx
	cdef int d_len

	d_len = len(d)
	vals = np.zeros(d_len, dtype=float)
	idx = 0
	for el in d.values():
		vals[idx] = el[0]
		idx += 1

	return vals
