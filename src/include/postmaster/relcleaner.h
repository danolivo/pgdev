/*
 * Relation cleaner
 */
#ifndef RELCLEANER_H
#define RELCLEANER_H

extern int rcleaner_start(void);

#ifdef EXEC_BACKEND
extern void RelCleanerMain(int argc, char *argv[]) pg_attribute_noreturn();
#endif

#endif							/* RELCLEANER_H */
