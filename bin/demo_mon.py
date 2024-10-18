#!/usr/bin/env python


from sam_globus_keepup.mon import ifstat


def main():
    print(ifstat())


if __name__ == '__main__':
    main()
