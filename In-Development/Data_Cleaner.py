import pandas as pd, argparse

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
    filtered_df = df[df['related IDs'] == 0]

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
    filtered_df = df[df['uploader'] == ""]
    
    return filtered_df


def main():
    # args = argparse.ArgumentParser()
    # args.add_argument("csv_file", nargs=1)
    # in_args = args.parse_args()
    # fileName = in_args.csv_file
    # fileName = fileName.remove(".csv")

    ## Reading CSV file using pandas
    try:
        file = pd.read_csv('0.csv')
        copy = file ## Copy of the file
        len_file_before = len(file)
        fileName = "0"

        broken, file = filter_broken_links(file)
        no_link = filter_no_links(file)
        no_length = filter_no_Length(file)
        no_uploader = filter_no_uploader(file)

        no_link2 = filter_no_links(copy)
        no_length2 = filter_no_Length(copy)
        no_uploader2 = filter_no_uploader(copy)

        print("Data Cleaner Stats".center(30))
        print("*"*30)
        print(f"Number of rows in {fileName}: {len(copy)}")
        print(f"Number of rows after Data Cleaning: {len(file)}")
        print()
        print("Clean Dataset / Original Dataset")
        print()
        print(f"Number of Broken Links: {len(broken)}")
        print(f"Percentage of {fileName}: {(len(broken) / len(copy))*100}%")
        print(f"Number of Videos with No Links: {len(no_link)} / {len(no_link2)}")
        print(f"Perecntage of {fileName}: {(len(no_link) / len(file))*100}% / {(len(no_link2) / len(copy))*100}%")
        print(f"Number of Videos with No Length: {len(no_length)} / {len(no_length2)}")
        print(f"Percentage of {fileName}: {(len(no_length) / len(file))*100}% / {(len(no_length2) / len(copy))*100}%")
        print(f"Number of videos with No Uploader: {len(no_uploader)} / {len(no_uploader2)}")
        print(f"Percentage of {fileName}: {(len(no_uploader) / len(file))}% / {(len(no_uploader2) / len(copy))}%")

        broken.to_csv(f"{fileName}_BrokenLinks", index=False)
        file.to_csv(f"{fileName}_Clean", index=False)
        #no_link.to_csv(f"{fileName}_NoLinks", index=False)
        #no_length.to_csv(f"{fileName}_NoLength", index=False)
        #no_uploader.to_csv(f"{fileName}_NoUploader", index=False)


    except Exception as e:
        print(f"Exception Error while opening file!: {e}")


if __name__ == "__main__":
    main()