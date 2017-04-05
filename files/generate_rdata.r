size = 100000
tmp <- matrix(rnorm(374 * size), size)
seq1 <- seq(1:size)
ndvi <- cbind(seq1,tmp)
save(ndvi, file = "/home/savio/R_workspace/ndvi_big_sample.RData")