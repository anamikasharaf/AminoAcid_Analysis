# AminoAcid_Analysis

Using MapReduce to find out frequency of each amino acid in FASTA file.

# Objective

Write a MapReduce program (including a driver, mapper, and reducer) that counts the frequency of each amino acid (and start and stop codons) in a given FASTA file for each of the 3 reading frames.

# Data

Concept of a reading frame can be found here -> https://en.wikipedia.org/wiki/Reading_frame  

Example: Given the sequence  AGGTGACACCGCAAGCCTTATATTAGC, there are 3 possible reading frames:

AGG TGA CAC CGC AAG CCT TAT ATT AGC

A GGT GAC ACC GCA AGC CTT ATA TTA GC

AG GTG ACA CCG CAA GCC TTA TAT TAG C

codon2aa.txt file is in the repository

## Mapper

Mapper maps protein found (refer: condon2aa.txt ) in all three frames for a sequence. 

## Reducer

Reducer counts the total number of presence of each protein in all three frames.

## Output

Output looks like this

Alanine			    3381454	3215080	3211077

Arginine		    3928264	3730207	3733975

Asparagine		  2936220	2784133	2790388

Aspartic acid		1758262	1670500	1668606

Cysteine		2647046	2514091	2515827

Glutamic acid		2854530	2715951	2716251

Glutamine		3046292	2895104	2894122

Glycine			4166742	3954067	3954684

Histidine		2544916	2414470	2414611

Isoleucine		4338850	4120383	4119275

Leucine			8447464	8033631	8034291

Lysine			4364002	4149451	4146707

Methionine		1368056	1303051	1302118

Phenylalanine		4376879	4157966	4158808

Proline			4153642	3948281	3949687

Serine			6964239	6610067	6612036

Stop codons		4008143	3806777	3809253

Threonine		3868541	3674922	3668828

Tryptophan		1456170	1383705	1382291

Tyrosine		2337305	2221116	2223533


The first column is the string derived exactly from the codon2aa.txt file, and the columns are the frequencies for the first, second, and third reading frames, respectively.



