import pandas as pd, argparse, time

def filter_broken_links(df):
    '''
        Function that filters out all broken links from the data.
        Returns a DataFrame object containing all broken links, and an Updated DataFrame
    '''
    non_null_count = df.notnull().sum(axis=1) # Checks to see how many of the columns of each row are null
    broken_links = df[non_null_count <= 1]
    update_data = df[non_null_count > 1]

    return broken_links, update_data

def filter_no_links(df):
    '''
        Function that filters out all rows that contain no reference links with their object.
        Return a DataFrame containing all rows that satisfy this condition
    '''
    filtered_df = df[df['related IDs'].isnull()]

    return filtered_df

def filter_no_Length(df):
    '''
        Function that filters out all rows that contain a video that has no length to it.
        Return a DataFrame of all videos satisfying this condition.
    '''
    filtered_df = df[df['length'] == 0]

    return filtered_df

def filter_no_uploader(df):
    '''
        Function that filters out all rows that do not contain an uploader.
        Returns a DataFrame of all videos satisfying this condition
    '''
    filtered_df = df[df['uploader'].isnull()]
    
    return filtered_df


def main():
    args = argparse.ArgumentParser()
    args.add_argument("csv_file", nargs=1)
    in_args = args.parse_args()
    f = in_args.csv_file
    fileName = f[0]

    ## Reading CSV file using pandas
    try:
        file = pd.read_csv(f[0])
        copy = file ## Copy of the file
        len_file_before = len(file)

        print(f"Data Cleaner Stats: {fileName}")
        print(f"*"*(len(f"Data Cleaner Stats: {fileName}")))
        start1 = time.time()
        broken, file = filter_broken_links(file)
        end = time.time()
        no_link = filter_no_links(file)
        no_length = filter_no_Length(file)
        no_uploader = filter_no_uploader(file)
        end1 = time.time()

        no_link2 = filter_no_links(copy)
        no_length2 = filter_no_Length(copy)
        no_uploader2 = filter_no_uploader(copy)
        end2 = time.time()

        broken.to_csv(f"BrokenLinks_{fileName}", index=False)
        file.to_csv(f"Clean_{fileName}", index=False)
        #no_link.to_csv(f"{fileName}_NoLinks", index=False)
        #no_length.to_csv(f"{fileName}_NoLength", index=False)
        #no_uploader.to_csv(f"{fileName}_NoUploader", index=False)
        end3 = time.time()

        print(f"Number of rows in {fileName}: {len(copy)}")
        print(f"Number of rows after Data Cleaning: {len(file)}")
        print(f"Number of rows cleaned in {fileName}: {len(copy) - len(file)}\n")

        print("Clean Dataset / Original Dataset\n")

        print(f"Number of Broken Links: {len(broken)}")
        print(f"Percentage of {fileName}: {(len(broken) / len(copy))*100:.2}%\n")
        print(f"Number of Videos with No Links: {len(no_link)} / {len(no_link2)}")
        print(f"Perecntage of {fileName}: {(len(no_link) / len(file))*100:.2}% / {(len(no_link2) / len(copy))*100:.2}%\n")
        print(f"Number of Videos with No Length: {len(no_length)} / {len(no_length2)}")
        print(f"Percentage of {fileName}: {(len(no_length) / len(file))*100:.2}% / {(len(no_length2) / len(copy))*100:.2}%\n")
        print(f"Number of videos with No Uploader: {len(no_uploader)} / {len(no_uploader2)}")
        print(f"Percentage of {fileName}: {(len(no_uploader) / len(file)):.2}% / {(len(no_uploader2) / len(copy)):.2}%\n")

        print(f"Time Analysis - Operation on Original File: {(end - start1) + (end2 - end1):.3}s")
        print(f"Time Analysis - Operation on Clean File: {(end1 - start1):.3}s")
        print(f"Time Analysis - Clean and other Files created: {end3 - end2:.3}s")

    except Exception as e:
        print(f"Exception Error while opening file!: {e}")


if __name__ == "__main__":
    main()