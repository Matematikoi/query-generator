# Fossil

To add a feature or solve an issue with the codebase we follow this
ideal:
1. Create a Ticket.
2. Make a branch mentioning the ticket in the first commit.
3. Add code.
4. Commit using `pixi run commit`.
5. Add tests.
6. If necessary add documenation in the wiki.
7. Talk with a team member to review the code and merge to trunk
(code review).

## Fossil UI
To access the ui (important to check tickets and wiki). You can
access the UI using the binary `.fossil` using
```bash
fossil ui binary_file_name.fossil
```

You can also `cd` into the a check-out of the project and activate it
```bash
cd path/to/src/code
fossil ui
```

## Wiki
The wiki can be access in the fossil UI inside the `Wiki` tab

## Ticket
The tickets can be acess in the fossil UI inside the `Tickets` tab.

Tickets have a `status` that can be 
1. Open
2. On going
3. Done
4. Deferred

It also has a `priority` that can be
1. High
2. Low
3. Nice to have

A nice description is always recommended when assigning the ticket
to somebody else.

## Commit and fossil sync
To update the local wiki and tickets you can run `fossil sync`. 
This will not push any changes that were done to the files that 
are being track.

To add new files you can use `fossil add path/to/file`. This will
track them. **All tracked files are added to the commit by default**

To commit you can run `pixi run commit`. We add this precaution to make
sure that the code can at least pass the basic tests.

If you want to **create a new branch** then you can easily do so
by commiting your changes with `pixi run commit -branch new_branch_name`

## Fossil update
To change branches you can run `fossil update branch_name` . 
You can also use `fossil update` to sync the changes you have 
with the ones on the server.

## Example

A common day would go like this

```bash
# Gets the latest wiki/tickets
fossil sync 
# ...
# Make some changes in the files
# ...


# Adds the new file
fossil add new/file


# make sure to include the ticket hash like [b144038aac]
pixi run commit -branch new_branch 

# After the code review merge
fossil update trunk
fossil merge new_branch
pixi run commit

# Finally update the ticket to DONE
```
