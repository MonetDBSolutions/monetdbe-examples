"""
(c) 2020 MonetDB Solutions All rights reserved
author: M.L. Kersten

This code module provides a version of mclient for MonetDB/e.
Its functionality is incrementally constructed.
"""
import time
import json
import monetdbe
import argparse


def usage():
    print("\\<file   - read input from file")
    print("\\>file   - save response in file, or stdout if no file is given")
    #  print("\\|cmd    - pipe result to process, or stop when no command is given")
    print("\\b       - read a cookbook of precooked SQL queries or show content")
    print("\\s       - display the Holmes schema ")
    #  print("\\history - show the readline history")
    #  print("\\help    - synopsis of the SQL syntax")
    #  print("\\D table - dumps the table, or the complete database if none given.")
    #  print("\\d[Stvsfn]+ [obj] - list database objects, or describe if obj given")
    #  print("\\A       - enable auto commit")
    #  print("\\a       - disable auto commit")
    #  print("\\e       - echo the query in sql formatting mode")
    print("\\t       - set the timer {none,clock,performance} (none is default)")
    print("\\f       - format using renderer {fancy, csv, tab, sql, html, json, rowcount}")
    #  print("\\w#      - set maximal page width (-1=unlimited, 0=terminal width, >0=limit to num)")
    #  print("\r#      - set maximum rows per page (-1=raw)")
    #  print("\L file  - save client-server interaction")
    #  print("\X       - trace mclient code")
    print("\\q       - terminate session and quit mclient")


timerformat = None
formatter = 'fancy'
screenwidth = 80
outputfile = None


def dummy():
    return [[1], [2], [4]], None

book = {
    "test": "select @",
    ".dummy": dummy,
    "count_tables": "select count(*) from sys.tables",
}


def loadbook(fname):
    try:
        f = open(fname,"r")
        clist = f.readlines()
        for c in clist:
            cmd, sql = c.split(':')
            book.update({cmd: sql})
    except IOError as msg:
        print(f"Failed to open cookbook '{fname}': {msg}")

def showbook():
    for e in book.keys():
        print("%-20s:%s" %(e, book[e]))
        #  print(f"{e} :{book[e]}")


def render(header, tuples, style='fancy', headers=True, colsep='|', rowsep='\n', width=5, maxwidth=25, nullvalue=''):
    if not header and not tuples:
        return "Missing result set"
    if not style:
        style = 'csv'
    #  print(f"formatter '{style}'")
    #  print('HDR', header)
    #  print('TUP', tuples)
    header = [h.name for h in header]
    answer = ''
    try:
        if style == 'sql':
            hdr = f'INSERT into "table"('
            sep = ''
            for d in header:
                hdr = hdr + sep + d
                sep = ','
            hdr = hdr + ') VALUES'
            for t in tuples:
                answer += f"{hdr}{t};\n"
            return answer

        if style == 'list' or style == 'csv' or style == 'tabs':
            if headers:
                answer += colsep.join(header) + rowsep
            for t in tuples:
                answer += colsep.join([str(e) for e in t]) + rowsep
            return answer

        if style == 'line':
            size = max([len(t) for t in header])
            for t in tuples:
                for h, e in zip(header, t):
                    answer += "{0:<{size}} = {1:}\n".format(h, e, size=size)
                answer += '\n'
            return answer

        if style == 'keyvalue':
            size = max([len(t[0]) for t in tuples])
            for t in tuples:
                answer += "{0:>{w}}: {1:}\n".format(t[0], t[1], w=size)
            return answer

        if style == 'raw':
            if not tuples:
                return 'No result set'
            for r in tuples:
                answer += ('\t,'.join([str(e) for e in r])) + rowsep
            return answer

        if style == 'fancy':
            # create an adjusted column width
            nw = []
            if tuples:
                t = tuples[0]
                for h, e in zip(header, t):
                    est = max([width, len(str(e)), len(str(h))]) + 1
                    if est > maxwidth:
                        est = maxwidth
                    if width == 0 or est > width:
                        nw.append(est)
                    else:
                        nw.append(width)
            if headers:
                hdr = ''
                for h, w in zip(header, nw):
                    if len(str(h)) > w:
                        v = str(h[:w])
                    else:
                        v = "{0:<{width}}".format(h, width=w)
                    hdr = hdr + v
                answer += hdr + '\n'
                hdr = ''
                for h, w in zip(header, nw):
                    hdr = hdr + '-' * (w-1) + ' '
                answer += hdr + '\n'

            for t in tuples:
                row = ''
                for e, w in zip(t, nw):
                    if e is None:
                        e = nullvalue
                    if type(e) == str:
                        if len(str(e)) > w:
                            v = str(e[:w])
                        else:
                            v = "{0:<{width}} ".format(e, width=w-1)
                    else:
                        if len(str(e)) > w:
                            v = str(e[:w])
                        else:
                            v = "{0:>{width}} ".format(e, width=w-1)
                    row = row + v
                answer += row + '\n'
            return answer

        if style == 'html':
            answer += "<table> <thead>\n"
            row = "<tr>"
            for h in header:
                row = row + f"<th>{h}</th>"
            row = row + "</tr>"
            answer += row + '\n'
            answer += "</thead><tbody>\n"

            for t in tuples:
                row = "<tr>"
                for f in t:
                    if nullvalue and not f:
                        v = ''
                    else:
                        v = f
                    row = row + f"<td>{v}</td>"
                row = row + "</tr>"
                answer += row + '\n'
            answer += "</tbody></table>\n"
            return answer

        if style == 'json':
            answer = "[\n"
            comma = ''
            for t in tuples:
                row = {}
                for h, e in zip(header, t):
                    if not e and nullvalue:
                        row.update({str(h): nullvalue})
                    else:
                        row.update({str(h): e})
                answer += f"{comma}{json.dumps(row, indent=4)}\n"
                comma = ','
            answer += "]\n"
    except Exception as msg:
        print(f"general exception {msg}")
    return answer


def fetchall(qry, debug=True):
    curr = conn.cursor()
    try:
        res = curr.execute(qry)
        rows = res.fetchall()
        if debug:
            print(f"answer:{rows}")
        return rows, res.description
    except monetdbe.DatabaseError as msg:
        print('ERROR fetchone:', msg)
    return None, None


def mclient_interpreter(lines):
    global timerformat, formatter, screenwidth, outputfile
    for line in lines:
        if line == '\\q':
            break
        if line == '\\?':
            usage()
            continue
        if line.startswith('\\>'):
            outputfile = line[2:].strip()
            print(f"Output redirected to file '{outputfile}'")
            continue

        if line.startswith('\\<'):
            inputfile = line[2:].strip()
            try:
                inp = open(inputfile, "r")
                lines = inp.readlines()
                mclient_interpreter(lines)
            except IOError as msg:
                print(f"Could not open inputfile '{inputfile}': {msg}")
            continue

        if line.startswith('\\b'):
            bookfile = line[2:].strip()
            if bookfile:
                print(f"Book from file '{bookfile}'")
                loadbook(bookfile)
            else:
                showbook()
            continue

        if line.startswith('\\t'):
            timerformat = line[2:].strip()
            if timerformat not in ['clock', 'performance', '']:
                print(f"Timer not in ['clock', 'performance', '']")
                return
            if not timerformat:
                timerformat = None
            print(f"Timer set to '{timerformat}'")
            continue

        if line.startswith('\\f'):
            formatter = line[2:].strip()
            if formatter not in ['fancy', 'csv', 'tab', 'raw', 'sql', 'html', 'json', 'rowcount', '']:
                print(f"Formatter not in ['fancy', 'csv', 'tab', 'raw', 'sql', html', 'json', 'rowcount', '']")
                return
            if not formatter:
                formatter = None
            print(f"Format set to '{formatter}'")
            continue

        if line.startswith('\\w'):
            try:
                screenwidth = int(line[2:].strip())
            except ValueError as msg:
                print(f"Screen width should be an integer:{msg}")
                continue

        try:
            # first check the cookbook for a commando
            cmd = line.split(' ')[0]
            #  print('cmd', cmd)

            if cmd == 'help' or line[0] == '?':
                for e in book.keys():
                    print(f"{e} :{book[e]}")
                continue

            # we can also call a python function
            if cmd in book and cmd[0] == '.':
                rows, hdr = book[cmd]()
                print(render(hdr, rows, style=formatter))
                continue

            if cmd in book:
                line = line[len(cmd):].strip()
                #  print(f"split '{line}'")
                if line:
                    args = line.split(',')
                else:
                    args = []
                #  print(f"argument'{args}'")

                line = book[cmd]
                #  print(f"precooked '{line}'")
                # cmd v1,v2,... are filled in
                for a in args:
                    line = line.replace('@', a)

            if line.find('@') >= 0:
                print('missing arguments', line)
                continue

            #  print('action', line)
            clk = time.time()
            rows, hdr = fetchall(line)

            clk = time.time() - clk
            if rows:
                if formatter == 'rowcount':
                    print(f"{len(rows)}")
                elif outputfile:
                    try:
                        out = open(outputfile, "w")
                        out.write(render(hdr, rows, style=formatter))
                    except IOError as msg:
                        print(f"Could not open outputfile '{outputfile}': {msg}")
                else:
                    print(render(hdr, rows, style=formatter), end=None)
                    #  db.print(rows)

            if line.startswith('\\|'):
                print(f"Sent result to process")

            if timerformat == 'clock':
                print(f"clk: {'%5.6f' % (clk * 1000) } ms")
        except monetdbe.DatabaseError as msg:
            print('Error:', msg)


def mclient():
    print(f"Welcome to mclient, the MonetDB/SQL interactive terminal (unreleased)")
    print("Type \\q to quit, \\? for a list of available commands")
    print("auto commit mode: on")

    # mimic mclient
    while True:
        line = input('> ')
        if not line:
            continue
        if line == '\\q':
            break
        mclient_interpreter([line])

if __name__ == "__main__":
    desc = f"Mclient simulator \n" \
           f"After database connection is established a mini mclient is started"\
           f"A collection of parameterised queries are defined in a book, but also" \
           f"freeformat queries can be formulated"
    arg_parser = argparse.ArgumentParser(description=desc)
    arg_parser.add_argument('-d', '--database', type=str, default=":memory:", help="Location of the database")

    args = arg_parser.parse_args()

    print('establish contact', args)
    conn = monetdbe.connect(args.database)
    if not conn:
        print(f"Could not access the memory {args.database}")
        exit -1
    
    mclient()
