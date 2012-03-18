use Sque;
use Sque::Worker;

my $s = Sque->new( stomp => '127.0.0.1:61613' );

$s->push( my_queue => {
    class => 'Test::Worker',
    args => [ 'Hello world!' ]
});

my $w = Sque::Worker->new( sque => $s, verbose => 1 );
$w->add_queues('my_queue');
$w->work;
