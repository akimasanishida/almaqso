temporary_file = 'import_analysisUtils_success.temp'

try:
    import analysisUtils as au
    # Write to temporary file: importing analysisUtils succeeds
    with open(temporary_file, 'w') as f:
        f.write('True\n')
except ImportError:
    # Write to temporary file: importing analysisUtils fails
    with open(temporary_file, 'w') as f:
        f.write('False\n')
