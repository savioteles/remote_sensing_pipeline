output = "/tmp/data_pieces"
input = "/home/savio/R_workspace/bfast/ndvi_sample_nobreak.rdata"
args = commandArgs(trailingOnly=TRUE)
input = if(!is.na(args[1])) as.character(args[1]) else input
output = if(!is.na(args[2])) as.character(args[2]) else output

load(input)
ndvi = as.matrix(mm2)

row_size = 375
block_size = 3
size = nrow(ndvi)

init = 1
file_index = 1

while (init < size) {
  end = min(init+block_size, size)
  
  ndvi_part = ndvi[init:end,]
  file_output_path = paste(output, "/", "piece_", file_index, ".RData", sep = "")
  cat(file_output_path, "\n")
  save(ndvi_part, file = file_output_path)
  
  file_index = file_index + 1
  init = init + block_size + 1
}

load("/tmp/data_pieces/piece_3.RData")
