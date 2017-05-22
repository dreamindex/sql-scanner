#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Digest::MD5 qw(md5_hex);

our $self = {};
sub main {
    my $commands = {
        filename => {
            test    => '.*\.log$',
            key     => 'filename',
        },
        display_count => {
            test    => '^\d+$',
            key     => 'display_count',
        }
    };
    foreach my $option ( @ARGV ) {
        foreach my $test ( keys %$commands ) {
            if ( $option =~ m/$commands->{$test}->{'test'}/ ) {
                $self->{$commands->{$test}->{'key'}} = $option;
                delete $commands->{$test};
            }
        }
    }
    &top_offenders($self->{'display_count'} || 5);
}

&main;

sub parse_log {
    return $self->{'_parsed_log'} ||= do {
        open(my $fh, '<:encoding(UTF-8)', $self->{'filename'})
            or die "Could not open file '$self->{'filename'}' $!";
        my $entries = {};
        my $entry = {statement => ''};
        while (my $row = <$fh>) {
            next if $row =~ m/^SET timestamp=/;
            if ( $entry->{'statement'} && ( $entry->{'statement'} =~ m/;$/ || $row =~ m/^# Time/) ) {
                $entry->{'statement'} =~ s/[\n|\s]+/ /g;
                my $md5_ref = md5_hex($entry->{'statement'});
                $entries->{$md5_ref}->{'count'} ++;
                $entries->{$md5_ref}->{'ref'} = $md5_ref;
                $entries->{$md5_ref}->{'statement'} = $entry->{'statement'};
                $entries->{$md5_ref}->{'tables'} = &tables_in_statement($entry->{'statement'});
                $entries->{$md5_ref}->{'query_time'} += $entry->{'query_time'};
                $entries->{$md5_ref}->{'lock_time'} += $entry->{'lock_time'};
                $entries->{$md5_ref}->{'rows_sent'} += $entry->{'rows_sent'};
                $entries->{$md5_ref}->{'rows_examined'} += $entry->{'rows_examined'};
                $entry = {};
            }
            if( $row =~ m/^# Time/ ) {
                ($entry->{'time'}) = $row =~ /# Time: (\d{6} \d\d:\d\d:\d\d)/;
            }
            elsif( $row =~ m/^# User/ ) {
                next;
            }
            elsif( $row =~ m/^# Query_time/ ) {
                ($entry->{query_time}, $entry->{'lock_time'}, $entry->{'rows_sent'}, $entry->{'rows_examined'}) = $row =~ /# Query_time: (\d+)  Lock_time: (\d+)  Rows_sent: (\d+)  Rows_examined: (\d+)/;
            }
            elsif( $row =~ m/^[^#]/ && $entry->{'time'}) {
                next if ! $entry->{'time'};
                $row=~s/IN\s?\([\d|,]+\)/IN(values)/;
                $row =~ s/=\s?\d+/= ?/g;
                $row =~ s/'[\w|\@|\.|%]+'/'?'/g;
                $row =~ s/'\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d'/'time'/g;
                $entry->{'statement'} = $row if ! $entry->{'statement'};
                $entry->{'statement'} .= $row if $row && $entry->{'statement'} !~ m/\Q$row\E/;
            }
            chomp $row;
        }
        foreach my $ref ( keys %$entries ) {
            $entries->{$ref}->{'query_time_average'} = $entries->{$ref}->{'query_time'} / ($entries->{$ref}->{'count'} || 1);
            $entries->{$ref}->{'lock_time_average'} = $entries->{$ref}->{'lock_time'} / ($entries->{$ref}->{'count'} || 1);
        }
        $entries;
    }
}

sub tables_in_statement {
    my $sql = shift;
    my ($tables) = $sql =~ /FROM\s(.+)\sWHERE/i;
    if ( ! $tables ) {
        ($tables) = $sql =~ /OPTIMIZE TABLE (\w+);/i;
    }
    if ( !$tables ) {
        my (@list) = $sql =~ /(\w+)\.\w/g;
        $tables = \@list;
    }
    $tables = [$tables] if ref $tables ne 'ARRAY';
    if ( ! $tables->[0] ) {
        ($tables->[0]) = $sql =~ /UPDATE\s([^\s]+)\sSET/i;
    }
    if ( ! $tables->[0] ) {
        ($tables->[0]) = $sql =~ /INSERT(?:.+)?\s([^\s]+)\s\(/i;
    }
    if ( ! $tables->[0] ) {
        ($tables->[0]) = $sql =~ /FROM\s(.+);/i;
    }
    if ($sql =~ m/^use/) {
        return [];
    }
    if ( ! $tables->[0] ) {
        print Dumper $sql;
        exit;
    }
    return $tables;
}

sub stats_by_table {
    return $self->{'_by_table'} ||= do {
        my $by_table = {};
        my $total = {};
        #re-org data by table
        foreach my $entry ( values %{&parse_log} ) {
            $total->{'start_time'} = $entry->{'time'} if ! $total->{'start_time'};
            $total->{'end_time'} = $entry->{'time'};
            $total->{query_time} += $entry->{'query_time'};
            $total->{lock_time} += $entry->{'lock_time'};
            $total->{count} += $entry->{'count'};
            $by_table->{ ( $entry->{'tables'}->[0] || 'unknown') }->{query_time} += ($entry->{'query_time'} || 0);
            $by_table->{ ( $entry->{'tables'}->[0] || 'unknown') }->{lock_time} += ($entry->{'lock_time'} || 0);
            $by_table->{ ( $entry->{'tables'}->[0] || 'unknown') }->{count} += ($entry->{'count'} || 0);
            $by_table->{ ( $entry->{'tables'}->[0] || 'unknown') }->{table} = ($entry->{'tables'} || 0);
        }
        #get aggregates
        foreach my $table (keys %$by_table) {
            $by_table->{$table}->{'percent_lock_time'} = sprintf("%.3f", 100 * $by_table->{$table}->{'lock_time'} / ($total->{'lock_time'} || 1) );
            $by_table->{$table}->{'percent_query_time'} = sprintf("%.3f", 100 * $by_table->{$table}->{'query_time'} / ($total->{'query_time'} || 1) );
        }
        $by_table;
    }
}

sub order_by_query_impact {
    my $column = shift;
    my $allowed = {
        lock_time => 1,
        lock_time_average => 1,
        query_time_average => 1,
        query_time => 1,
    };
    return [] if ! $allowed->{ $column };
    my @log_data = values %{&parse_log};
    my @ordered = reverse sort { $a->{$column} <=> $b->{$column} } @log_data;
    return \@ordered;
}

sub order_by_table_impact {
    &stats_by_table;
    my $column = shift;
    my $allowed = {
        percent_lock_time   => 1,
        percent_query_time  => 1,
        lock_time           => 1,
        query_time          => 1
    };
    return [] if ! $allowed->{ $column };
    my @log_data = values %{&stats_by_table};
    my @ordered = reverse sort { $a->{$column} <=> $b->{$column} } @log_data;
    return \@ordered;
}

sub top_offenders {
    my $rows = shift;
    print "#######################SCANNING LOG#################################\n";
    my @tables_most_likey_to_lock = splice @{&order_by_table_impact('percent_lock_time')}, 0, $rows;
    print "#######################Tables/joins most likely to be in a locked query#################################\n";
    print Dumper \@tables_most_likey_to_lock;
    my @tables_most_likey_to_stall = splice @{&order_by_table_impact('percent_query_time')}, 0, $rows;
    print "#######################Tables/joins most likely to be in a slow query#################################\n";
    print Dumper \@tables_most_likey_to_stall;
    print "#######################Cause of most locks by query#################################\n";
    my @lock = splice @{&order_by_query_impact('lock_time')}, 0, $rows;
    print Dumper \@lock;
    print "#######################Worst lockers by query#################################\n";
    my @worst_lock = splice @{&order_by_query_impact('lock_time_average')}, 0, $rows;
    print Dumper \@worst_lock;
    print "#######################Highest Latency Queries#################################\n";
    my @slow = splice @{&order_by_query_impact('query_time')}, 0, $rows;
    print Dumper \@slow;
    print "#######################Slowest Queries#################################\n";
    my @worst_query = splice @{&order_by_query_impact('query_time_average')}, 0, $rows;
    print Dumper \@worst_query;
}
