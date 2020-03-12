 #!/usr/bin/perl -w

use strict;
use warnings;

srand(0);

my $RECORD_COUNT = 10000000;
for(my $i = 1; $i <= $RECORD_COUNT; $i++) {
    my @field;
    push(@field, $i);
    push(@field, sprintf("%08d\@example.com", $i));
    push(@field, int(rand(100))+1);
    push(@field, int(rand(10000000))+1);
    print join("\t", @field), "\n";
}
