package Sque::Worker;
{
  $Sque::Worker::VERSION = '0.001';
}
use Any::Moose;
use Any::Moose '::Util::TypeConstraints';
with 'Sque::Encoder';

use POSIX ":sys_wait_h";
use Sys::Hostname;
use Scalar::Util qw(blessed weaken);
use List::MoreUtils qw{ uniq any };
use DateTime;
use Try::Tiny;

# ABSTRACT: Does the hard work of babysitting Sque::Job's

use overload
    '""' => \&_string,
    '==' => \&_is_equal,
    'eq' => \&_is_equal;

has sque => (
    is => 'ro',
    required => 1,
    handles => [qw/ stomp key /]
);

has queues => (
    is => 'rw',
    isa => 'ArrayRef',
    lazy => 1,
    default => sub {[]}
);

has id => ( is => 'rw', lazy => 1, default => sub { $_[0]->_stringify } );
sub _string { $_[0]->id } # can't point overload to a mo[o|u]se attribute :-(

has verbose => ( is => 'rw', default => sub {0} );

has cant_fork => ( is => 'rw', default => sub {0} );

has child => ( is => 'rw' );

has shutdown => ( is => 'rw', default => sub{0} );

has paused => ( is => 'rw', default => sub{0} );
has interval => ( is => 'rw', default => sub{5} );

sub pause { $_[0]->paused(1) }

sub unpause { $_[0]->paused(0) }

sub shutdown_please {
    print "Shutting down...\n";
    $_[0]->shutdown(1);
}

sub shutdown_now { $_[0]->shutdown_please && $_[0]->kill_child }

sub work {
    my $self = shift;
    $self->startup;
    while ( ! $self->shutdown ) {
        if ( !$self->paused && ( my $job = $self->reserve ) ) {
            $self->log("Got job $job");
            $self->work_tick($job);
        }
        elsif( $self->interval ) {
            my $status = $self->paused ? "Paused" : 'Waiting for ' . join( ', ', @{$self->queues} );
            $self->procline( $status );
            $self->log( $status );
            sleep $self->interval;
        }
    }
}

sub work_tick {
    my ($self, $job) = @_;

    $self->working_on($job);
    my $timestamp = DateTime->now->strftime("%Y/%m/%d %H:%M:%S %Z");

    if ( !$self->cant_fork && ( my $pid = fork ) ) {
        $self->procline( "Forked $pid at $timestamp" );
        $self->child($pid);
        $self->log( "Waiting for $pid" );
        #while ( ! waitpid( $pid, WNOHANG ) ) { } # non-blocking has sense?
        waitpid( $pid, 0 );
        $self->log( "Forked job($pid) exited with status $?" );
    }
    else {
        $self->procline( sprintf( "Processing %s since %s", $job->queue, $timestamp ) );
        $self->perform($job);
        exit(0) unless $self->cant_fork;
    }

    $self->done_working;
    $self->child(0);
}


sub perform {
    my ( $self, $job ) = @_;
    my $ret;
    try {
        $ret = $job->perform;
        $self->log( sprintf( "done: %s", $job->stringify ) );
    }
    catch {
        $self->log( sprintf( "%s failed: %s", $job->stringify, $_ ) );
        #$job->fail($_);
    };
    $ret;
}

sub kill_child {
    my $self = shift;
    return unless $self->child;
    if ( kill 0, $self->child ) {
        $self->log( "Killing my child: " . $self->child );
        kill 9, $self->child;
    }
    else {
        $self->log( "Child " . $self->child . " not found, shutting down." );
        $self->shutdown_please;
    }
}

sub add_queue {
    my $self = shift;
    return unless @_;
    my $queue = $self->sque->key( @_ );
    $self->queues( [ uniq( @{$self->queues}, $queue ) ] );
    $self->_subscribe_queue( $queue );
}

sub del_queue {
    my ( $self, $queue ) = @_;
    return unless $queue;
    
    return
    @{$self->queues}
           -
    @{$self->queues( [ grep {$_} map { $_ eq $queue ? undef : $_ } @{$self->queues} ] )};
}


sub next_queue {
    my $self = shift;
    if ( @{$self->queues} > 1 ) {
        push @{$self->queues}, shift @{$self->queues};
    }
    return $self->queues->[-1];
}

sub reserve {
    my $self = shift;
    return $self->sque->pop;
}

sub working_on {
    my ( $self, $job ) = @_;
    $job->worker($self);
}

sub done_working {
    my $self = shift;
    #$self->processed(1);
    # TODO: Ack stomp message
}

sub _stringify {
    my $self = shift;
    join ':', hostname, $$, join( ',', @{$self->queues} );
}

# Is this worker the same as another worker?
sub _is_equal {
    my ($self, $other) = @_;
    $self->id eq $other->id;
}

sub procline {
    my $self = shift;
    if ( my $str = shift ) {
        $0 = sprintf( "sque-%s: %s", $Resque::VERSION || 'devel', $str );
    }
    $0;
}

sub startup {
    my $self = shift;
    $0 = 'sque: Starting';

    $self->register_signal_handlers;
}

sub register_signal_handlers {
    my $self = shift;
    weaken $self;
    $SIG{TERM} = sub { $self->shutdown_now };
    $SIG{INT} = sub { $self->shutdown_now };
    $SIG{QUIT} = sub { $self->shutdown_please };
    $SIG{USR1} = sub { $self->kill_child };
    $SIG{USR2} = sub { $self->pause };
    $SIG{CONT} = sub { $self->unpause };
}

sub worker_pids {
    my $self = shift;
    my @pids;
    for ( split "\n", `ps axo pid,command | grep sque` ) {
        if ( m/^\s*(\d+)\s(.+)$/ ) {
            push @pids, $1;
        }
    }
    return wantarray ? @pids : \@pids;
}

#TODO: add logger() attr to containg a logger object and if set, use that instead of print!
sub log {
    my $self = shift;
    return unless $self->verbose;
    print STDERR shift, "\n";
}

sub _subscribe_queue {
    my $self = shift;
    $self->stomp->subscribe(
        destination => @_,
        ack         => 'client',
    );
};

__PACKAGE__->meta->make_immutable();

1;

__END__
=pod

=head1 NAME

Sque::Worker - Does the hard work of babysitting Sque::Job's

=head1 VERSION

version 0.001

=head1 ATTRIBUTES

=head2 sque

The L<Sque> object running this worker.

=head2 queues

Queues this worker should fetch jobs from.

=head2 id

Unique identifier for the running worker.
Used to set process status all around.

The worker stringify to this attribute.

=head2 verbose

Set to a true value to make this worker report what's doing while
on work().

=head2 cant_fork

Set it to a true value to stop this worker from fork jobs.

By default, the worker will fork the job out and control the
children process. This make the worker more resilient to
memory leaks.

=head2 child

PID of current running child.

=head2 shutdown

When true, this worker will shutdown after finishing current job.

=head2 paused

When true, this worker won't proccess more jobs till false.

=head1 METHODS

=head2 pause

Stop processing jobs after the current one has completed (if we're
currently running one).

=head2 unpause

Start processing jobs again after a pause

=head2 shutdown_please

Schedule this worker for shutdown. Will finish processing the
current job.

=head2 shutdown_now

Kill the child and shutdown immediately.

=head2 work

Calling this method will make this worker to start pulling & running jobs
from queues().

This is the main wheel and will run while shutdown() is false.

=head2 work_tick

Perform() one job and wait till it finish.

=head2 perform

Call perform() on the given Sque::Job capturing and reporting
any exception.

=head2 kill_child

Kills the forked child immediately, without remorse. The job it
is processing will not be completed.

=head2 add_queue

Add a queue this worker should listen to.

=head2 del_queue

Stop listening to the given queue.

=head2 next_queue

Circular iterator over queues().

=head2 reserve

Pull the next job to be precessed.

=head2 working_on

Set worker and working status on the given L<Sque::Job>.

=head2 done_working

Inform the backend this worker has done its current job

=head2 procline

Given a string, sets the procline ($0) and logs.
Procline is always in the format of:
sque-VERSION: STRING

=head2 startup

Helper method called by work() to:
1. register_signal_handlers()

=head2 register_signal_handlers

Registers the various signal handlers a worker responds to.
TERM: Shutdown immediately, stop processing jobs.
INT: Shutdown immediately, stop processing jobs.
QUIT: Shutdown after the current job has finished processing.
USR1: Kill the forked child immediately, continue processing jobs.
USR2: Don't process any new jobs
CONT: Start processing jobs again after a USR2

=head2 worker_pids

Returns an Array of string pids of all the other workers on this
machine. Useful when pruning dead workers on startup.

=head2 log

If verbose() is true, this will print to STDERR.

=head1 AUTHOR

William Wolf <throughnothing@gmail.com>

=head1 COPYRIGHT AND LICENSE


William Wolf has dedicated the work to the Commons by waiving all of his
or her rights to the work worldwide under copyright law and all related or
neighboring legal rights he or she had in the work, to the extent allowable by
law.

Works under CC0 do not require attribution. When citing the work, you should
not imply endorsement by the author.

=cut

