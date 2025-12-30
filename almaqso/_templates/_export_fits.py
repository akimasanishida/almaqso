import os
import glob

input_dir = "{dir}"
output_dir = input_dir + "_fits"

if not os.path.exists(output_dir):
    os.mkdir(output_dir)

for image_file in glob.glob(f"{{input_dir}}/*.image.pbcor"):
    base_name = os.path.basename(image_file).replace(".image.pbcor", "")
    fits_file = os.path.join(output_dir, f"{{base_name}}.fits")
    exportfits(imagename=image_file, fitsimage=fits_file)
