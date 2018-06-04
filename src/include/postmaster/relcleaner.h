/*
 * Relation cleaner
 */
#ifndef RELCLEANER_H
#define RELCLEANER_H

extern int relcleaner_start(void);

extern void RelCleanerMain(void) pg_attribute_noreturn();

#endif							/* RELCLEANER_H */
