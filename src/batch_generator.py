#  Membertools, a basic set of tools for administering lists, by Martin Keegan

#  Copyright (C) 2013  Martin Keegan
#
#  This programme is free software; you may redistribute and/or modify
#  it under the terms of the Apache Software Licence v2.0

import itertools

def batch_generator(size, g):
	buffer = []
	try:
		while True:
			for i in range(0, size):
				buffer.append(g.next())
			yield buffer
			buffer = []
	except StopIteration:
		if 0 == len(buffer):
			raise
		else:
			yield buffer
